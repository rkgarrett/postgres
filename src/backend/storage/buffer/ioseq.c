/*-------------------------------------------------------------------------
 *
 * ioseq.c
 *
 *	  Routines for sorting dirty buffers to form sequential runs.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "access/xlog.h"
#include "storage/ioseq.h"

/* GUC's sort_dirty_buffers */
bool sortDirtyBuffers = true;

/* Array of buffer indices/tags in shared memory */
BufferIdxTag *ioSeqList;

/*
 *----------------------------------------------------------------------
 * CompareTags --
 *
 *		Comparison function that lets us group same-file buffers
 *		together and sort on block numbers within the file. It requires
 *		BufferTag to have no padding bytes and blockNum to be the last
 *		field in BufferTag. See IOSort_Sort for the corresponding asserts.
 *----------------------------------------------------------------------
 */
static int
CompareTags(const void *e1, const void *e2)
{
	const BufferTag *t1 = &((const BufferIdxTag *) e1)->tag;
	const BufferTag *t2 = &((const BufferIdxTag *) e2)->tag;
	int retval = memcmp(t1, t2, offsetof(BufferTag, blockNum));

	if (retval == 0)
		retval = t1->blockNum - t2->blockNum;

	return retval;
}


/*
 *----------------------------------------------------------------------
 * CompareClocks --
 *
 *      Comparison function that puts soon-to-be-evicted buffers earlier
 *      in the buffer. It preserves sequential runs (they have the
 *      same clock value) and orders by block number within each run.
 *----------------------------------------------------------------------
 */static int
CompareClocks(const void *v1, const void *v2)
{
	const BufferIdxTag *e1 = (const BufferIdxTag *) v1;
	const BufferIdxTag *e2 = (const BufferIdxTag *) v2;
	int retval = e1->clock - e2->clock;

	if (retval == 0)
		retval = e1->tag.blockNum - e2->tag.blockNum;

	return retval;
}

/*
 *----------------------------------------------------------------------
 * SeqEntries --
 *
 *		Returns true iff e2 belongs to the same file as e1 and its block
 *		immediately follows e1.
 *----------------------------------------------------------------------
 */
static inline bool
SeqEntries(const BufferIdxTag *e1, const BufferIdxTag *e2)
{
	return memcmp(&e1->tag, &e2->tag, offsetof(BufferTag, blockNum)) == 0 &&
		e1->tag.blockNum + 1 == e2->tag.blockNum;
}

/*
 *----------------------------------------------------------------------
 * ClockVal --
 *
 *		Returns the eviction priority of a given buffer index. Buffer
 *		that will be evicted next has ClockVal == 0.
 *----------------------------------------------------------------------
 */
static inline int
ClockVal(int idx, int startIdx)
{
	idx -= startIdx;
	return (idx >= 0) ? idx : idx + NBuffers;
}

/*
 *----------------------------------------------------------------------
 * IOSeqVerify --
 *
 *      Check that the buffer list is properly sorted: if two buffers
 *      belong to the same file and are sequential, they better be
 *      next to each other.
 *----------------------------------------------------------------------
 */
static inline void
IOSeqVerify(BufferIdxTag *entries, unsigned numEntries)
{
#if 0
	static unsigned count;

	if (count++ % 10 == 0)
	{
		unsigned i;

		for (i = 0; i < numEntries; i++)
		{
			unsigned j;

			for (j = 0; j < numEntries; j++)
				Assert(!SeqEntries(&entries[i], &entries[j]) || j == i + 1);
		}
	}
#endif
}

/*
 *----------------------------------------------------------------------
 * IOSeq_ShmemSize --
 *
 *		Compute the size of shared memory for the list of buffer
 *		indices/tags.
 *----------------------------------------------------------------------
 */
Size
IOSeq_ShmemSize(void)
{
	return mul_size(NBuffers, sizeof(BufferIdxTag));
}

/*
 *----------------------------------------------------------------------
 * IOSeq_Init --
 *
 *		Initialize ioSeqList to point to the list of buffers'
 *		indices/tags in shared memory. This is called once during
 *		shared-memory initialization (either in the postmaster, or in
 *		a standalone backend).
 *----------------------------------------------------------------------
 */
void
IOSeq_Init(void)
{
	bool found;

	ioSeqList = (BufferIdxTag *)
		ShmemInitStruct("Sorted Buffers", IOSeq_ShmemSize(),
						&found);
}

//XXX: should not be here.
extern bool doubleWrites;

/*
 *----------------------------------------------------------------------
 * IOSeq_Sort --
 *
 *      Sort the passed-in list of buffers to group same-file
 *      sequential buffers together but try to preserve the clock
 *      order, to some extent.
 *----------------------------------------------------------------------
 */
void
IOSeq_Sort(BufferIdxTag *entries, unsigned numEntries)
{
	unsigned i = 0;
	int firstVictim;

	/*
	 * Sort entries to put same-file blocks together. A couple of
	 * asserts on the structure layout:
	 * 1. BufferTag has no pad bytes -> can use memcmp in CompareTags.
	 * 2. blockNum is the last field of BufferTag -> can use offsetof.
	 */
	Assert(sizeof(BufferTag) ==
		   3 * sizeof(Oid) + sizeof(ForkNumber) + sizeof(BlockNumber) &&
		   sizeof(BufferTag) ==
		   offsetof(BufferTag, blockNum) + sizeof(BlockNumber));
	qsort(entries, numEntries, sizeof(BufferIdxTag), CompareTags);

	/* Assign each sequential run one clock value (min of its members) */
	firstVictim = StrategySyncStart(NULL, NULL);

	while (i < numEntries)
	{
		int minClock = ClockVal(entries[i].idx, firstVictim);
		unsigned j = i + 1;

		for (; j < numEntries && SeqEntries(&entries[j-1], &entries[j]); j++)
		{
			int clock = ClockVal(entries[j].idx, firstVictim);

			if (minClock > clock)
			{
				minClock = clock;
			}
		}

		/*
		 * This loop also advances i past the sequential run found, so
		 * execute it, even if we're not going to do the second qsort.
		 */
		for (; i < j; i++)
			entries[i].clock = minClock;
	}

	/*
	 * In the case of double writes with buffered IO, don't do the
	 * second sort, so batches are more likely to be to just a single
	 * file, and so we only have to do one fdatasync for each batch.
	 */
	if (!doubleWrites)
	{
		/* Re-sort runs based on the clock */
		qsort(entries, numEntries, sizeof(BufferIdxTag), CompareClocks);
	}

	IOSeqVerify(entries, numEntries);
}


/*
 *----------------------------------------------------------------------
 * IOSeq_ClockStart --
 *
 *		Finds the first dirty buffer in the clock order and returns
 *		its position in the entries array. NB: Assumes that the
 *		entries array is in the original order (increasing buf ids),
 *		rather than sorted via IOSeq_Sort.
 *----------------------------------------------------------------------
 */
unsigned
IOSeq_ClockStart(BufferIdxTag *entries, unsigned numEntries)
{
	int val = StrategySyncStart(NULL, NULL); /* not necessarily dirty */
	unsigned left = 0, mid, right = numEntries;

	/* Binary-search to find the nearest entry with id >= val or numEntries. */
	while (left != right)
	{
		mid = (left + right) / 2;
		if (entries[mid].idx < val)
			left = mid + 1;
		else
			right = mid;
	}

#ifdef USE_ASSERT_CHECKING
	/* Assert the entries are sorted by idx. Also verify the binary-search. */
	for (mid = numEntries; mid != 0 && entries[mid - 1].idx >= val; mid--)
		Assert(mid == numEntries || entries[mid].idx > entries[mid - 1].idx);
	Assert(mid == left);
#endif
	/* If nextVictim is after the last dirty buffer, first dirty is at 0. */
	if (left == numEntries)
		left = 0;

	return left;
}
