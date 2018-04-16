/*-------------------------------------------------------------------------
 *
 * smgr.c
 *	  public interface routines to storage manager switch.
 *
 *	  All file system operations in POSTGRES dispatch through these
 *	  routines.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "catalog/catalog.h"
#include "commands/tablespace.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"


/*
 * This struct of function pointers defines the API between smgr.c and
 * any individual storage manager module.  Note that smgr subfunctions are
 * generally expected to report problems via elog(ERROR).  An exception is
 * that smgr_unlink should use elog(WARNING), rather than erroring out,
 * because we normally unlink relations during post-commit/abort cleanup,
 * and so it's too late to raise an error.  Also, various conditions that
 * would normally be errors should be allowed during bootstrap and/or WAL
 * recovery --- see comments in md.c for details.
 */
typedef struct f_smgr
{
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileNodeBackend rnode, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum);
	void		(*smgr_read) (SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, char *buffer);
	void		(*smgr_write) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_bwrite) (int writeLen, struct SMgrWriteList *writeList,
								File doubleWriteFile, char *doubleBuf);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_pre_ckpt) (void);	/* may be NULL */
	void		(*smgr_sync) (void);	/* may be NULL */
	void		(*smgr_post_ckpt) (void);	/* may be NULL */
} f_smgr;


static const f_smgr smgrsw[] = {
	/* magnetic disk */
	{mdinit, NULL, mdclose, mdcreate, mdexists, mdunlink, mdextend,
	    mdprefetch, mdread, mdwrite, mbdwrite, mdwriteback, mdnblocks,
	    mdtruncate, mdimmedsync, mdpreckpt, mdsync, mdpostckpt
	}
};

static const int NSmgr = lengthof(smgrsw);


/*
 * Each backend has a hashtable that stores all extant SMgrRelation objects.
 * In addition, "unowned" SMgrRelation objects are chained together in a list.
 */
static HTAB *SMgrRelationHash = NULL;

/* Page checksumming. */
static uint64 tempbuf[BLCKSZ / sizeof(uint64)];
static bool FlushPreviousBatches(struct DWBufferInfo *dwi, int endFill);

#define INVALID_CKSUM 0x1b0af034

static SMgrRelation first_unowned_reln = NULL;

/* local function prototypes */
static void smgrshutdown(int code, Datum arg);
static void add_to_unowned_list(SMgrRelation reln);
static void remove_from_unowned_list(SMgrRelation reln);

/*
 * Buffer used to write the double-write header.  Useful to align buffer
 * to page size if it is written with direct IO.
 */
#define CC_ALIGN(_LEN) __attribute__ ((aligned (_LEN)))
static char doubleBuf[DOUBLE_WRITE_HEADER_SIZE] CC_ALIGN(4096);

/* Files to which double writes are going */
File		doubleWriteFile[16];

/* Info on the double-write buffers */
struct DWBufferInfo *dwInfo;
/* Blocks of the double-write buffers */
char *dwBlocks;


/*
 *	smgrinit(), smgrshutdown() -- Initialize or shut down storage
 *								  managers.
 *
 * Note: smgrinit is called during backend startup (normal or standalone
 * case), *not* during postmaster start.  Therefore, any resources created
 * here or destroyed in smgrshutdown are backend-local.
 */
void
smgrinit(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_init)
			smgrsw[i].smgr_init();
	}

	/* register the shutdown proc */
	on_proc_exit(smgrshutdown, 0);
}

/*
 * on_proc_exit hook for smgr cleanup during backend shutdown
 */
static void
smgrshutdown(int code, Datum arg)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_shutdown)
			smgrsw[i].smgr_shutdown();
	}
}

/*
 *	smgropen() -- Return an SMgrRelation object, creating it if need be.
 *
 *		This does not attempt to actually open the underlying file.
 */
SMgrRelation
smgropen(RelFileNode rnode, BackendId backend)
{
	RelFileNodeBackend brnode;
	SMgrRelation reln;
	bool		found;

	if (SMgrRelationHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileNodeBackend);
		ctl.entrysize = sizeof(SMgrRelationData);
		SMgrRelationHash = hash_create("smgr relation table", 400,
									   &ctl, HASH_ELEM | HASH_BLOBS);
		first_unowned_reln = NULL;
	}

	/* Look up or create an entry */
	brnode.node = rnode;
	brnode.backend = backend;
	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  (void *) &brnode,
									  HASH_ENTER, &found);

	/* Initialize it if not present before */
	if (!found)
	{
		int			forknum;

		/* hash_search already filled in the lookup key */
		reln->smgr_owner = NULL;
		reln->smgr_targblock = InvalidBlockNumber;
		reln->smgr_fsm_nblocks = InvalidBlockNumber;
		reln->smgr_vm_nblocks = InvalidBlockNumber;
		reln->smgr_which = 0;	/* we only have md.c at present */

		/* mark it not open */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			reln->md_num_open_segs[forknum] = 0;

		/* it has no owner yet */
		add_to_unowned_list(reln);
	}

	return reln;
}

/*
 * smgrsetowner() -- Establish a long-lived reference to an SMgrRelation object
 *
 * There can be only one owner at a time; this is sufficient since currently
 * the only such owners exist in the relcache.
 */
void
smgrsetowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* We don't support "disowning" an SMgrRelation here, use smgrclearowner */
	Assert(owner != NULL);

	/*
	 * First, unhook any old owner.  (Normally there shouldn't be any, but it
	 * seems possible that this can happen during swap_relation_files()
	 * depending on the order of processing.  It's ok to close the old
	 * relcache entry early in that case.)
	 *
	 * If there isn't an old owner, then the reln should be in the unowned
	 * list, and we need to remove it.
	 */
	if (reln->smgr_owner)
		*(reln->smgr_owner) = NULL;
	else
		remove_from_unowned_list(reln);

	/* Now establish the ownership relationship. */
	reln->smgr_owner = owner;
	*owner = reln;
}

/*
 * smgrclearowner() -- Remove long-lived reference to an SMgrRelation object
 *					   if one exists
 */
void
smgrclearowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* Do nothing if the SMgrRelation object is not owned by the owner */
	if (reln->smgr_owner != owner)
		return;

	/* unset the owner's reference */
	*owner = NULL;

	/* unset our reference to the owner */
	reln->smgr_owner = NULL;

	add_to_unowned_list(reln);
}

/*
 * add_to_unowned_list -- link an SMgrRelation onto the unowned list
 *
 * Check remove_from_unowned_list()'s comments for performance
 * considerations.
 */
static void
add_to_unowned_list(SMgrRelation reln)
{
	/* place it at head of the list (to make smgrsetowner cheap) */
	reln->next_unowned_reln = first_unowned_reln;
	first_unowned_reln = reln;
}

/*
 * remove_from_unowned_list -- unlink an SMgrRelation from the unowned list
 *
 * If the reln is not present in the list, nothing happens.  Typically this
 * would be caller error, but there seems no reason to throw an error.
 *
 * In the worst case this could be rather slow; but in all the cases that seem
 * likely to be performance-critical, the reln being sought will actually be
 * first in the list.  Furthermore, the number of unowned relns touched in any
 * one transaction shouldn't be all that high typically.  So it doesn't seem
 * worth expending the additional space and management logic needed for a
 * doubly-linked list.
 */
static void
remove_from_unowned_list(SMgrRelation reln)
{
	SMgrRelation *link;
	SMgrRelation cur;

	for (link = &first_unowned_reln, cur = *link;
		 cur != NULL;
		 link = &cur->next_unowned_reln, cur = *link)
	{
		if (cur == reln)
		{
			*link = cur->next_unowned_reln;
			cur->next_unowned_reln = NULL;
			break;
		}
	}
}

/*
 *	smgrexists() -- Does the underlying file for a fork exist?
 */
bool
smgrexists(SMgrRelation reln, ForkNumber forknum)
{
	return smgrsw[reln->smgr_which].smgr_exists(reln, forknum);
}

/*
 *	smgrclose() -- Close and delete an SMgrRelation object.
 */
void
smgrclose(SMgrRelation reln)
{
	SMgrRelation *owner;
	ForkNumber	forknum;

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		smgrsw[reln->smgr_which].smgr_close(reln, forknum);

	owner = reln->smgr_owner;

	if (!owner)
		remove_from_unowned_list(reln);

	if (hash_search(SMgrRelationHash,
					(void *) &(reln->smgr_rnode),
					HASH_REMOVE, NULL) == NULL)
		elog(ERROR, "SMgrRelation hashtable corrupted");

	/*
	 * Unhook the owner pointer, if any.  We do this last since in the remote
	 * possibility of failure above, the SMgrRelation object will still exist.
	 */
	if (owner)
		*owner = NULL;
}

/*
 *	smgrcloseall() -- Close all existing SMgrRelation objects.
 */
void
smgrcloseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	hash_seq_init(&status, SMgrRelationHash);

	while ((reln = (SMgrRelation) hash_seq_search(&status)) != NULL)
		smgrclose(reln);
}

/*
 *	smgrclosenode() -- Close SMgrRelation object for given RelFileNode,
 *					   if one exists.
 *
 * This has the same effects as smgrclose(smgropen(rnode)), but it avoids
 * uselessly creating a hashtable entry only to drop it again when no
 * such entry exists already.
 */
void
smgrclosenode(RelFileNodeBackend rnode)
{
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  (void *) &rnode,
									  HASH_FIND, NULL);
	if (reln != NULL)
		smgrclose(reln);
}

/*
 *	smgrcreate() -- Create a new relation.
 *
 *		Given an already-created (but presumably unused) SMgrRelation,
 *		cause the underlying disk file or other storage for the fork
 *		to be created.
 *
 *		If isRedo is true, it is okay for the underlying file to exist
 *		already because we are in a WAL replay sequence.
 */
void
smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	/*
	 * Exit quickly in WAL replay mode if we've already opened the file. If
	 * it's open, it surely must exist.
	 */
	if (isRedo && reln->md_num_open_segs[forknum] > 0)
		return;

	/*
	 * We may be using the target table space for the first time in this
	 * database, so create a per-database subdirectory if needed.
	 *
	 * XXX this is a fairly ugly violation of module layering, but this seems
	 * to be the best place to put the check.  Maybe TablespaceCreateDbspace
	 * should be here and not in commands/tablespace.c?  But that would imply
	 * importing a lot of stuff that smgr.c oughtn't know, either.
	 */
	TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							isRedo);

	smgrsw[reln->smgr_which].smgr_create(reln, forknum, isRedo);
}

/*
 * Invalidate entries in the double-write buffer that are writing
 * to the specified relation.
 */
static void
InvalDoubleWriteBuffer(SMgrRelation reln, ForkNumber forknum, int dwIndex)
{
	struct DWBufferInfo *dwi = dwInfo + dwIndex;
	int endFill, i;

	LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
	endFill = dwi->endFill;
	if (FlushPreviousBatches(dwi, endFill))
	{
		/*
		 * We've flushed everything up to endFill, so nothing to invalidate.
		 */
		LWLockRelease(dwi->writeLock);
	}
	else
	{
		Assert(dwi->start <= endFill);
		for (i = dwi->start; i < endFill; i++)
		{
			struct SMgrWriteList *wr = &(dwi->writeList[i & dwi->mask]);
			if (memcmp(&(wr->smgr_rnode), &(reln->smgr_rnode),
					   sizeof(RelFileNodeBackend)) == 0 &&
				wr->forkNum == forknum)
			{
				elog(LOG, "Invalidating entry %d in dwbuf %d", i, dwIndex);
				wr->forkNum = InvalidForkNumber;
			}
		}
		LWLockRelease(dwi->writeLock);
	}
}

/*
 *	smgrdounlink() -- Immediately unlink all forks of a relation.
 *
 *		All forks of the relation are removed from the store.  This should
 *		not be used during transactional operations, since it can't be undone.
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 *
 *		This is equivalent to calling smgrdounlinkfork for each fork, but
 *		it's significantly quicker so should be preferred when possible.
 */
void
smgrdounlink(SMgrRelation reln, bool isRedo)
{
	RelFileNodeBackend rnode = reln->smgr_rnode;
	int			which = reln->smgr_which;
	ForkNumber	forknum;

	/* Close the forks at smgr level */
	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		smgrsw[which].smgr_close(reln, forknum);

	/*
	 * Get rid of any remaining buffers for the relation.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelFileNodesAllBuffers(&rnode, 1);

	/*
	 * It'd be nice to tell the stats collector to forget it immediately, too.
	 * But we can't because we don't know the OID (and in cases involving
	 * relfilenode swaps, it's not always clear which table OID to forget,
	 * anyway).
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for this rel.  We should do this
	 * before starting the actual unlinking, in case we fail partway through
	 * that step.  Note that the sinval message will eventually come back to
	 * this backend, too, and thereby provide a backstop that we closed our
	 * own smgr rel.
	 */
	CacheInvalidateSmgr(rnode);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */
	smgrsw[which].smgr_unlink(rnode, InvalidForkNumber, isRedo);
}

/*
 *	smgrdounlinkall() -- Immediately unlink all forks of all given relations
 *
 *		All forks of all given relations are removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 *
 *		This is equivalent to calling smgrdounlink for each relation, but it's
 *		significantly quicker so should be preferred when possible.
 */
void
smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo)
{
	int			i = 0;
	RelFileNodeBackend *rnodes;
	ForkNumber	forknum;

	if (nrels == 0)
		return;

	/*
	 * create an array which contains all relations to be dropped, and close
	 * each relation's forks at the smgr level while at it
	 */
	rnodes = palloc(sizeof(RelFileNodeBackend) * nrels);
	for (i = 0; i < nrels; i++)
	{
		RelFileNodeBackend rnode = rels[i]->smgr_rnode;
		int			which = rels[i]->smgr_which;

		rnodes[i] = rnode;

		/* Close the forks at smgr level */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_close(rels[i], forknum);
	}

	/*
	 * Get rid of any remaining buffers for the relations.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelFileNodesAllBuffers(rnodes, nrels);

	/*
	 * It'd be nice to tell the stats collector to forget them immediately,
	 * too. But we can't because we don't know the OIDs.
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for these rels.  We should do
	 * this before starting the actual unlinking, in case we fail partway
	 * through that step.  Note that the sinval messages will eventually come
	 * back to this backend, too, and thereby provide a backstop that we
	 * closed our own smgr rel.
	 */
	for (i = 0; i < nrels; i++)
		CacheInvalidateSmgr(rnodes[i]);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */

	for (i = 0; i < nrels; i++)
	{
		int			which = rels[i]->smgr_which;

		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_unlink(rnodes[i], forknum, isRedo);
	}

	pfree(rnodes);
}

/*
 *	smgrdounlinkfork() -- Immediately unlink one fork of a relation.
 *
 *		The specified fork of the relation is removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		If isRedo is true, it is okay for the underlying file to be gone
 *		already.
 */
void
smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	RelFileNodeBackend rnode = reln->smgr_rnode;
	int			which = reln->smgr_which;

	/* Close the fork at smgr level */
	smgrsw[which].smgr_close(reln, forknum);

	/*
	 * Get rid of any remaining buffers for the fork.  bufmgr will just drop
	 * them without bothering to write the contents.
	 */
	DropRelFileNodeBuffers(rnode, forknum, 0);

	if (doubleWrites)
	{
		/*
		 * Can invalidate one at a time, since relation is already
		 * cleared from the buffer cache.
		 */
		InvalDoubleWriteBuffer(reln, forknum, DWBUF_NON_CHECKPOINTER);
		InvalDoubleWriteBuffer(reln, forknum, DWBUF_CHECKPOINTER);
	}

	/*
	 * It'd be nice to tell the stats collector to forget it immediately, too.
	 * But we can't because we don't know the OID (and in cases involving
	 * relfilenode swaps, it's not always clear which table OID to forget,
	 * anyway).
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for this rel.  We should do this
	 * before starting the actual unlinking, in case we fail partway through
	 * that step.  Note that the sinval message will eventually come back to
	 * this backend, too, and thereby provide a backstop that we closed our
	 * own smgr rel.
	 */
	CacheInvalidateSmgr(rnode);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */
	smgrsw[which].smgr_unlink(rnode, forknum, isRedo);
}

/*
 * The initial value when computing the checksum for a data page.
 */
static inline uint64
ChecksumInit(SMgrRelation reln, ForkNumber f, BlockNumber b)
{
	return b + f;
}

/*
 * Compute a checksum of buffer (with length len), using initial value
 * cksum.  We use a relatively simple checksum calculation to avoid
 * overhead, but could replace with some kind of CRC calculation.
 */
static inline uint32
ComputeChecksum(uint64 *buffer, uint32 len, uint64 cksum)
{
	int			i;

	for (i = 0; i < len / sizeof(uint64); i += 4)
	{
		cksum += (cksum << 5) + *buffer;
		cksum += (cksum << 5) + *(buffer + 1);
		cksum += (cksum << 5) + *(buffer + 2);
		cksum += (cksum << 5) + *(buffer + 3);
		buffer += 4;
	}
	cksum = (cksum & 0xFFFFFFFF) + (cksum >> 32);
	return cksum;
}

/*
 * Copy buffer to dst and compute the checksum during the copy (so that
 * the checksum is correct for the final contents of dst).
 */
static inline uint32
CopyAndComputeChecksum(uint64 *dst, volatile uint64 *buffer,
					   uint32 len, uint64 cksum)
{
	int			i;

	for (i = 0; i < len / sizeof(uint64); i += 4)
	{
		cksum += (cksum << 5) + (*dst = *buffer);
		cksum += (cksum << 5) + (*(dst + 1) = *(buffer + 1));
		cksum += (cksum << 5) + (*(dst + 2) = *(buffer + 2));
		cksum += (cksum << 5) + (*(dst + 3) = *(buffer + 3));
		dst += 4;
		buffer += 4;
	}
	cksum = (cksum & 0xFFFFFFFF) + (cksum >> 32);
	return cksum;
}

/*
 *	smgrextend() -- Add a new block to a file.
 *
 *		The semantics are nearly the same as smgrwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   char *buffer, bool skipFsync)
{
	PageHeader	p;

	Assert(PageGetPageLayoutVersion(((PageHeader) buffer)) ==
		   PG_PAGE_LAYOUT_VERSION ||
		   PageIsNew(buffer));
	if (page_checksum)
	{
		p = (PageHeader) tempbuf;
		((PageHeader) buffer)->cksum = 0;

		/*
		 * We copy and compute the checksum, and then write out the data from
		 * the copy, so that we avoid any problem with hint bits changing
		 * after we compute the checksum.
		 */
		p->cksum = CopyAndComputeChecksum(tempbuf, (uint64 *) buffer, BLCKSZ,
									  ChecksumInit(reln, forknum, blocknum));
	}
	else
	{
		p = (PageHeader) buffer;
		p->cksum = INVALID_CKSUM;
	}
	smgrsw[reln->smgr_which].smgr_extend(reln, forknum, blocknum,
										 (char *) p, skipFsync);
}

/*
 *	smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
 */
void
smgrprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	smgrsw[reln->smgr_which].smgr_prefetch(reln, forknum, blocknum);
}

/*
 * Look for specified reln/fork/block in the double-write buffer.  Return
 * TRUE and the index in the buffer if found.
 */
static bool
CheckDoubleWriteBuffer(int dwIndex, SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, int *match)
{
	struct DWBufferInfo *dwi = dwInfo + dwIndex;
	int i;
	int start, endFill;

	/*
	 * Check without lock first.  If we see what we think is a match, we
	 * will check again with appropriate lock.
	 */
	pg_memory_barrier();
	start = dwi->start;
	endFill = dwi->endFill;
	for (i = start; i < endFill; i++)
	{
		struct SMgrWriteList *p = &(dwi->writeList[i & dwi->mask]);

		if (p->blockNum == blocknum &&
			memcmp(&(p->smgr_rnode), &(reln->smgr_rnode),
				   sizeof(RelFileNodeBackend)) == 0 &&
			p->forkNum == forknum) {
			break;
		}
	}
	if (i == endFill) {
		return FALSE;
	}

	LWLockAcquire(dwi->writeLock, LW_SHARED);
	start = dwi->start;
	endFill = dwi->endFill;
	Assert(start <= endFill);
	for (i = start; i < endFill; i++)
	{
		struct SMgrWriteList *p = &(dwi->writeList[i & dwi->mask]);

		if (p->blockNum == blocknum &&
			memcmp(&(p->smgr_rnode), &(reln->smgr_rnode),
				   sizeof(RelFileNodeBackend)) == 0 &&
			p->forkNum == forknum) {
			if (0) elog(LOG, "Found block in the double-write buffer %d", dwIndex);
			dwi->readHits++;
			*match = i;
			return TRUE;
		}
	}
	LWLockRelease(dwi->writeLock);
	return FALSE;
}

/*
 *	smgrread() -- read a particular block from a relation into the supplied
 *				  buffer.
 *
 *		This routine is called from the buffer manager in order to
 *		instantiate pages in the shared buffer cache.  All storage managers
 *		return pages in the format that POSTGRES expects.
 */
void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool *cksumMismatch)
{
	PageHeader	p = (PageHeader) buffer;

	if (doubleWrites)
	{
		int match;
		struct DWBufferInfo *dwi;

		/*
		 * Must look for the block first in the double-write buffers.
		 * Can check one at a time, since no one else can be reading the
		 * buffer into memory.
		 */
		dwi = dwInfo + DWBUF_CHECKPOINTER;
		if (CheckDoubleWriteBuffer(DWBUF_CHECKPOINTER, reln, forknum,
								   blocknum, &match))
		{
			memcpy(buffer, dwi->writeList[match & dwi->mask].buffer, BLCKSZ);
			LWLockRelease(dwi->writeLock);
			return;
		}
		dwi = dwInfo + DWBUF_NON_CHECKPOINTER;
		if (CheckDoubleWriteBuffer(DWBUF_NON_CHECKPOINTER, reln, forknum,
								   blocknum, &match))
		{
			memcpy(buffer, dwi->writeList[match & dwi->mask].buffer, BLCKSZ);
			LWLockRelease(dwi->writeLock);
				return;
		}
	}

	smgrsw[reln->smgr_which].smgr_read(reln, forknum, blocknum, buffer);

	Assert(PageIsNew(p) || PageGetPageLayoutVersion(p) == PG_PAGE_LAYOUT_VERSION);
	if (page_checksum && p->cksum != INVALID_CKSUM)
	{
		const uint32 diskCksum = p->cksum;
		uint32		cksum;

		p->cksum = 0;
		cksum = ComputeChecksum((uint64 *) buffer, BLCKSZ,
								ChecksumInit(reln, forknum, blocknum));
		if (cksum != diskCksum)
		{
			if (cksumMismatch != NULL)
			{
				*cksumMismatch = TRUE;
				return;
			}
			ereport(PANIC, (0, errmsg("checksum mismatch: disk has %#x, should be %#x\n"
				  "filename %s, BlockNum %u, block specifier %d/%d/%d/%d/%u",
									  diskCksum, (uint32) cksum,
									  relpath(reln->smgr_rnode, forknum),
									  blocknum,
									  reln->smgr_rnode.node.spcNode,
									  reln->smgr_rnode.node.dbNode,
									  reln->smgr_rnode.node.relNode,
									  forknum, blocknum)));
		}
	}
}

/*
 * If double-write file is not yet open in this process, open up the
 * double-write file.
 */
static void
CheckDoubleWriteFile(int index)
{
	if (doubleWriteFile[index] == 0)
	{
		char	   *name = DoubleWriteFileName(index);

		doubleWriteFile[index] = PathNameOpenFile(name,
												  O_RDWR | O_CREAT | O_DIRECT,
												  S_IRUSR | S_IWUSR);
		if (doubleWriteFile[index] < 0)
			elog(PANIC, "Couldn't open double-write file %s", name);
		pfree(name);
	}
}

/*
 * Flush blocks start to end-1 in the double-write buffer.
 */
static void
FlushDoubleWriteBatch(int dwIndex, int start, int end)
{
	struct DWBufferInfo * dwi = dwInfo + dwIndex;
	int fileIndex = (start & dwi->mask) / batched_buffer_writes +
		MAX_BATCHES_PER_DWBUF * dwIndex;

	CheckDoubleWriteFile(fileIndex);
	/*
	 * The partial write list we need is contiguous because
	 * batched_buffer_writes always evenly divides MAX_DW_BLOCKS.
	 */
	Assert(start < end && end - start <= batched_buffer_writes);
	//elog(LOG, "Flushing %d, %d", start, end);
	smgrbwrite(end - start, &(dwi->writeList[start & dwi->mask]),
			   doubleWriteFile[fileIndex]);
}

/*
 * Flush the batch ending at endFlush if no other process is doing it.
 * If doWait is TRUE, then wait until batch is definitely flushed.
 * endFlush should be the end of a batch (divisible by
 * batched_buffer_writes)
 */
static void
TryFlushBatch(int dwIndex, int endFlush, bool doWait)
{
	struct DWBufferInfo *dwi = dwInfo + dwIndex;
	LWLockId batchLock;
	int start;

	pg_memory_barrier();
	if (dwi->start >= endFlush)
	{
		return;
	}
	batchLock = dwi->batchLocks[endFlush & dwi->mask];
	/* XXX Race - might end up waiting for a later flush? */
	if (doWait)
	{
		/*
		 * Getting the batch lock will wait for batch to be flushed or
		 * allow us to do it.
		 */
		LWLockAcquire(batchLock, LW_EXCLUSIVE);
	}
	else
	{
		if (!LWLockConditionalAcquire(batchLock, LW_EXCLUSIVE))
		{
			/* Someone else is flushing -- let them do it... */
			return;
		}
	}

	if (dwi->start >= endFlush || dwi->flushed[endFlush & dwi->mask])
	{
		/* Some other process already flushed the batch */
		LWLockRelease(batchLock);
		return;
	}

	/* Find beginning of batch */
	start = endFlush - batched_buffer_writes;
	if (start < dwi->start)
	{
		start = dwi->start;
	}
	FlushDoubleWriteBatch(dwIndex, start, endFlush);
	LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
	if (start == dwi->start)
	{
		/*
		 * We flushed the oldest block, so let's advance dwi->start.
		 */
		dwi->start = endFlush;
		dwi->flushed[endFlush & dwi->mask] = 0;
		/*
		 * Keep advancing dwi->start if there were later blocks
		 * flushed.
		 */
		while (dwi->endFill >= endFlush + batched_buffer_writes &&
			   dwi->flushed[(endFlush + batched_buffer_writes) & dwi->mask])
		{
			endFlush += batched_buffer_writes;
			dwi->start = endFlush;
			dwi->flushed[endFlush & dwi->mask] = 0;
		}
	}
	else
	{
		/* Not the oldest block, so just mark that we flushed it. */
		dwi->flushed[endFlush & dwi->mask] = 1;
	}
	LWLockRelease(dwi->writeLock);
	LWLockRelease(batchLock);
}


/* Round down a to the boundary of a batch */
#define ROUND_DOWN_BATCH(a)  ((a) - ((a) % batched_buffer_writes))
#define ROUND_UP_BATCH(a)    ((a) + batched_buffer_writes - ((a) % batched_buffer_writes))

/*
 * Flush all batches previous to endFill.  Requires dwi->writeLock is
 * held.  Return TRUE, if, while waiting, we actually flushed the batch
 * containing endFill itself.
 */
static bool
FlushPreviousBatches(struct DWBufferInfo *dwi, int endFill)
{
	if (endFill == dwi->start) {
		/* Nothing to flush */
		return TRUE;
	}
	if (dwi->start >= ROUND_DOWN_BATCH(endFill)) {
		/* No previous batches to flush */
		return FALSE;
	}

	/*
	 * Other processes should already be flushing up to
	 * ROUND_DOWN_BATCH(endFill)
	 */
	LWLockRelease(dwi->writeLock);

	while (dwi->start < ROUND_DOWN_BATCH(endFill)) {
		//elog(LOG, "Waiting for flushes in FlushPreviousBatches");
		TryFlushBatch(dwi - dwInfo, ROUND_UP_BATCH(dwi->start), TRUE);
	}
	LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
	if (ROUND_DOWN_BATCH(dwi->endFill) >= endFill) {
		/* dwi->endFill has now advanced past endFill's batch, so we can
		 * wait for someone to flush past endFill */
		if (dwi->start < endFill) {
			LWLockRelease(dwi->writeLock);
			while (dwi->start < endFill) {
				//elog(LOG, "Waiting for last flush in FlushPreviousBatches");
				TryFlushBatch(dwi - dwInfo, ROUND_UP_BATCH(dwi->start), TRUE);
			}
			LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
		}
		return TRUE;
	}
	return FALSE;
}

/*
 * Check if there is a duplicate for the specified reln/fork/block
 * already in the double-write buffer.  If so, handle it (wait for it to
 * be flushed or copy over previous contents with current contents).
 */
static void
CheckDuplicate(int dwIndex, SMgrRelation reln, ForkNumber forknum,
			   BlockNumber blocknum, char *buffer)
{
	int match;

	if (CheckDoubleWriteBuffer(dwIndex, reln, forknum, blocknum, &match))
	{
		struct DWBufferInfo *dwi = dwInfo + dwIndex;

		elog(LOG, "Duplicate block in dwbuf [%d] %d (%d %d)",
			 dwIndex, match, dwi->start, dwi->endFill);
		if (match < ROUND_DOWN_BATCH(dwi->endFill))
		{
			/* Duplicate is in previous batch, so wait for that to be flushed. */
			LWLockRelease(dwi->writeLock);
			while (dwi->start <= match)
			{
				elog(LOG, "Waiting for flush because of duplicate");
				TryFlushBatch(dwIndex, ROUND_UP_BATCH(dwi->start), TRUE);
			}
		}
		else
		{
			/*
			 * Duplicate is in this batch, just copy over previous
			 * contents, so both writes will do the same thing (instead
			 * of removing previous write, which will be harder to do).
			 */
			memcpy(buffer, dwi->writeList[match & dwi->mask].buffer, BLCKSZ);
			LWLockRelease(dwi->writeLock);
		}
	}
}

/*
 * Flush double-write buffer up to its current fill length
 */
void
FlushDoubleWriteBuffer(int dwIndex)
{
	int endFill;
	struct DWBufferInfo *dwi = dwInfo + dwIndex;

	LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
	endFill = dwi->endFill;
	if (FlushPreviousBatches(dwi, endFill))
	{
		LWLockRelease(dwi->writeLock);
		return;
	}

	/* We must flush the last partial batch */
	if (0) elog(LOG, "Flushing partial batch [%d] %d, %d %d %d %d in FlushDoubleWriteBuffer",
				dwIndex, endFill - dwi->start,
				dwi->start, endFill, dwi->endFill, dwi->endAlloc);
	Assert(endFill < dwi->start + batched_buffer_writes);
	/*
	 * Keep write lock while flushing partial batch, so no one else
	 * can add to the batch and complete it.
	 */
	FlushDoubleWriteBatch(dwIndex, dwi->start, endFill);
	dwi->flushed[ROUND_UP_BATCH(dwi->start) & dwi->mask] = 0;
	dwi->start = endFill;
	LWLockRelease(dwi->writeLock);
}

/*
 *	smgrwrite() -- Write the supplied buffer out.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use smgrextend().
 *
 *		This is not a synchronous write -- the block is not necessarily
 *		on disk at return, only dumped out to the kernel.  However,
 *		provisions will be made to fsync the write before the next checkpoint.
 *
 *		skipFsync indicates that the caller will make other provisions to
 *		fsync the relation, so we needn't bother.  Temporary relations also
 *		do not require fsync.
 */
void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  char *buffer, bool skipFsync, bool dw)
{
	PageHeader	p;
	int myBlock = 0;
	int dwIndex = 0;
	struct DWBufferInfo *dwi = NULL;

	dw = dw && doubleWrites;
	if (dw)
	{
		dwIndex = AmCheckpointer() ? DWBUF_CHECKPOINTER : DWBUF_NON_CHECKPOINTER;
		dwi = &dwInfo[dwIndex];

		/*
		 * Since no other process can be writing out this buffer, we
		 * just need to check the double-write buffers for duplicates
		 * once upfront.
		 */
		CheckDuplicate(DWBUF_CHECKPOINTER, reln, forknum, blocknum, buffer);
		CheckDuplicate(DWBUF_NON_CHECKPOINTER, reln, forknum, blocknum, buffer);

		if (dwIndex == DWBUF_CHECKPOINTER)
		{
			/* No locking needed */
			myBlock = dwi->endAlloc++;
		}
		else
		{
			/*
			 * Get the next DW block that we can copy into.
			 * allocLock ensures there is no starvation, and we
			 * will get a block once one is available.
			 */
			LWLockAcquire(dwi->allocLock, LW_EXCLUSIVE);
			if (dwi->endAlloc - dwi->start >= dwi->length)
			{
				//elog(LOG, "Waiting for too many blocks in double-write buffer");
				dwi->fullWaits++;
				TryFlushBatch(DWBUF_NON_CHECKPOINTER, ROUND_UP_BATCH(dwi->start),
							  TRUE);
				pg_memory_barrier();
				Assert(dwi->endAlloc - dwi->start < dwi->length);
			}
			/*
			 * Now that we've allocated an entry, get the write lock to
			 * copy in the buffer and add the new entry.
			 */
			LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
			Assert(dwi->endAlloc >= dwi->endFill);
			myBlock = dwi->endAlloc++;
		}
		p = (PageHeader)(dwi->bufferStart + BLCKSZ * (myBlock & dwi->mask));
	}
	else if (page_checksum)
	{
		/* Checksums to tempbuf, since no double-writing. */
		p = (PageHeader) tempbuf;
	}
	else
	{
		/* No checksums or double-writing. */
		p = (PageHeader) buffer;
	}

	if (page_checksum)
	{
		((PageHeader) buffer)->cksum = 0;

		/*
		 * We copy and compute the checksum, and then write out the data from
		 * the copy, so that we avoid any problem with hint bits changing
		 * after we compute the checksum.
		 */
		p->cksum = CopyAndComputeChecksum((uint64 *)p, (uint64 *) buffer,
										  BLCKSZ,
										  ChecksumInit(reln, forknum, blocknum));
	}
	else
	{
		if (dw)
		{
			/* No checksum, just copy to double-write buffer */
			memcpy(p, buffer, BLCKSZ);
		}
		p->cksum = INVALID_CKSUM;
	}

	Assert(PageGetPageLayoutVersion(p) == PG_PAGE_LAYOUT_VERSION);

	if (dw)
	{
		int i;
		struct SMgrWriteList *wr;
		int endFlush;
		bool tryFlush;

		if (dwIndex == DWBUF_CHECKPOINTER)
		{
			/* For the checkpointer, we hadn't gotten writeLock yet. */
			LWLockAcquire(dwi->writeLock, LW_EXCLUSIVE);
		}
		Assert(dwi->start <= dwi->endFill);

		/* Add info on the block we want to write to the writeList */
		i = dwi->endFill++;
		wr = &(dwi->writeList[i & dwi->mask]);
		wr->smgr_rnode = reln->smgr_rnode;
		wr->forkNum = forknum;
		wr->blockNum = blocknum;
		wr->buffer = (char *)p;
		wr->slot = myBlock & dwi->mask;

		endFlush = dwi->endFill;
		tryFlush = (endFlush > dwi->start && (endFlush % batched_buffer_writes) == 0);
		LWLockRelease(dwi->writeLock);
		if (dwIndex == DWBUF_NON_CHECKPOINTER)
		{
			LWLockRelease(dwi->allocLock);
		}
		if (tryFlush)
		{
			/* We filled out a batch, so let's flush it */
			TryFlushBatch(dwIndex, endFlush, FALSE);
		}
	}
	else
	{
		smgrsw[reln->smgr_which].smgr_write(reln, forknum, blocknum,
											(char *) p, skipFsync);
	}
}

/*
 * Write out a list of buffers, as specified in writeList.	If
 * doubleWriteFile is >= 0, then also do double writes to the specified
 * file (so full_page_writes can be avoided).
 */
void
smgrbwrite(int writeLen, struct SMgrWriteList *writeList,
		   File doubleWriteFile)
{
	(*(smgrsw[reln->smgr_which].smgr_write)) (reln, forknum, blocknum,
											  buffer, skipFsync);
	PageHeader	p = NULL;
	int			i;

	/*
	 * Set up the initial double-write page that lists all the buffers
	 * that will be written to the double write file This list includes
	 * the checksums of all the buffers and is checksummed itself.
	 */
	struct DoubleBufHeader *hdr = (struct DoubleBufHeader *) doubleBuf;
	struct DoubleBufItem *item = hdr->items;

	/*
	 * The double-write header size should be big enough to contain info
	 * for up to MAX_BATCH_BLOCKS buffers.
	 */
	Assert(sizeof(struct DoubleBufHeader) + MAX_BATCH_BLOCKS * sizeof(struct DoubleBufItem) <= DOUBLE_WRITE_HEADER_SIZE);

	/* Remove any invalidated entries from the batch. */
	for (i = 0; i < writeLen; ) {
		if (writeList[i].forkNum == InvalidForkNumber)
		{
			elog(LOG, "Removing entry %d from dwbuf batch", i);
			if (i < writeLen - 1) {
				memcpy(&writeList[i], &writeList[i+1], sizeof(struct SMgrWriteList) * (writeLen - 1 -i));
			}
			writeLen--;
		}
		else
		{
			i++;
		}
	}

	for (i = 0; i < writeLen; i++)
	{
		item->rnode = writeList[i].smgr_rnode;
		item->forkNum = writeList[i].forkNum;
		item->blockNum = writeList[i].blockNum;
		item->slot = writeList[i].slot;
		p = (PageHeader) writeList[i].buffer;
		item->cksum = p->cksum;
		item->pd_lsn = p->pd_lsn;
		item++;

		writeList[i].localrel = smgropen(writeList[i].smgr_rnode.node,
										 writeList[i].smgr_rnode.backend);
	}
	hdr->writeLen = writeLen;
	hdr->cksum = ComputeChecksum((uint64 *) hdr, DOUBLE_WRITE_HEADER_SIZE, 0);

	/* XXX */
	(*(smgrsw[0].smgr_bwrite)) (writeLen, writeList,
												 doubleWriteFile, doubleBuf);
	/* Zero out part of header that we filled in. */
	memset(doubleBuf, 0,
		   (char *) &(((struct DoubleBufHeader *) doubleBuf)->items[writeLen]) - doubleBuf);
}

/*
 *	smgrwriteback() -- Trigger kernel writeback for the supplied range of
 *					   blocks.
 */
void
smgrwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  BlockNumber nblocks)
{
	smgrsw[reln->smgr_which].smgr_writeback(reln, forknum, blocknum,
											nblocks);
}

/*
 *	smgrnblocks() -- Calculate the number of blocks in the
 *					 supplied relation.
 */
BlockNumber
smgrnblocks(SMgrRelation reln, ForkNumber forknum)
{
	return smgrsw[reln->smgr_which].smgr_nblocks(reln, forknum);
}

/*
 *	smgrtruncate() -- Truncate supplied relation to the specified number
 *					  of blocks
 *
 * The truncation is done immediately, so this can't be rolled back.
 */
void
smgrtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	DropRelFileNodeBuffers(reln->smgr_rnode, forknum, nblocks);

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel.  This is useful because they
	 * might have open file pointers to segments that got removed, and/or
	 * smgr_targblock variables pointing past the new rel end.  (The inval
	 * message will come back to our backend, too, causing a
	 * probably-unnecessary local smgr flush.  But we don't expect that this
	 * is a performance-critical path.)  As in the unlink code, we want to be
	 * sure the message is sent before we start changing things on-disk.
	 */
	CacheInvalidateSmgr(reln->smgr_rnode);

	/*
	 * Do the truncation.
	 */
	smgrsw[reln->smgr_which].smgr_truncate(reln, forknum, nblocks);
}

/*
 *	smgrimmedsync() -- Force the specified relation to stable storage.
 *
 *		Synchronously force all previous writes to the specified relation
 *		down to disk.
 *
 *		This is useful for building completely new relations (eg, new
 *		indexes).  Instead of incrementally WAL-logging the index build
 *		steps, we can just write completed index pages to disk with smgrwrite
 *		or smgrextend, and then fsync the completed index file before
 *		committing the transaction.  (This is sufficient for purposes of
 *		crash recovery, since it effectively duplicates forcing a checkpoint
 *		for the completed index.  But it is *not* sufficient if one wishes
 *		to use the WAL log for PITR or replication purposes: in that case
 *		we have to make WAL entries as well.)
 *
 *		The preceding writes should specify skipFsync = true to avoid
 *		duplicative fsyncs.
 *
 *		Note that you need to do FlushRelationBuffers() first if there is
 *		any possibility that there are dirty buffers for the relation;
 *		otherwise the sync is not very meaningful.
 */
void
smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	smgrsw[reln->smgr_which].smgr_immedsync(reln, forknum);
}


/*
 *	smgrpreckpt() -- Prepare for checkpoint.
 */
void
smgrpreckpt(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_pre_ckpt)
			smgrsw[i].smgr_pre_ckpt();
	}
}

/*
 *	smgrsync() -- Sync files to disk during checkpoint.
 */
void
smgrsync(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_sync)
			smgrsw[i].smgr_sync();
	}
}

/*
 *	smgrpostckpt() -- Post-checkpoint cleanup.
 */
void
smgrpostckpt(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_post_ckpt)
			smgrsw[i].smgr_post_ckpt();
	}
}


extern char *double_write_directory;

/*
 * Return the name of the double write file.  Caller must use pfree().
 */
char *
DoubleWriteFileName(int index)
{
	char	   *name;

	if (double_write_directory != NULL)
	{
		name = palloc(strlen(double_write_directory) + strlen("/double") + 2 + 1);
		sprintf(name, "%s/double.%d", double_write_directory, index);
	}
	else
	{
		name = palloc(strlen("base") + strlen("/double") + 2 + 1);
		sprintf(name, "base/double.%d", index);
	}
	return name;
}

/*
 * Called by postmaster at startup during recovery to read double-write
 * file and see if there are any data blocks to be recovered.
 */
void
RecoverDoubleWriteFile()
{
	char	   *name;
	struct stat stat_buf;
	File		fd;
	int			r;
	struct DoubleBufHeader *hdr;
	int			i;
	uint32		savedCksum;

	name = DoubleWriteFileName(0);
	if (stat(name, &stat_buf) == -1)
	{
		elog(LOG, "No double-write file");
		pfree(name);
		return;
	}
	fd = PathNameOpenFile(name, O_RDONLY, S_IRUSR | S_IWUSR);
	if (fd < 0)
	{
		elog(PANIC, "Double-write file %s exists but can't be opened", name);
	}

	r = FileRead(fd, doubleBuf, DOUBLE_WRITE_HEADER_SIZE);
	if (r < 0)
		goto badformat;

	hdr = (struct DoubleBufHeader *) doubleBuf;
	if (hdr->writeLen == 0 && hdr->cksum == 0)
	{
		elog(LOG, "Double-write file %s is zeroed out", name);
		FileClose(fd);
		pfree(name);
		return;
	}
	if (hdr->writeLen < 1 || hdr->writeLen > MAX_BATCH_BLOCKS)
		goto badformat;

	savedCksum = hdr->cksum;
	hdr->cksum = 0;
	if (savedCksum !=
		ComputeChecksum((uint64 *) hdr, DOUBLE_WRITE_HEADER_SIZE, 0))
		goto badformat;

	for (i = 0; i < hdr->writeLen; i++)
	{
		struct DoubleBufItem *it = &(hdr->items[i]);
		SMgrRelation smgr;
		bool		mismatch;
		PageHeader	p;

		/*
		 * For each block described in double-write file header, see if the
		 * block in the database file has a checksum mismatch.	If so, restore
		 * the block from the double-write file if that entry has correct
		 * checksum.
		 */
		smgr = smgropen(it->rnode.node, it->rnode.backend);
		mismatch = false;

		/*
		 * The block may no longer exist if relation was deleted/truncated
		 * after the last double-write.
		 */
		if (!smgrexists(smgr, it->forkNum) ||
			it->blockNum >= smgrnblocks(smgr, it->forkNum))
		{
			elog(LOG, "Block %s/%d in slot %d of double-write file no longer exists, skipping",
				 relpath(it->rnode, it->forkNum), it->blockNum, i);
			continue;
		}

		smgrread(smgr, it->forkNum, it->blockNum, (char *) tempbuf, &mismatch);
		if (mismatch)
		{
			/*
			 * The corresponding data block has a checksum error, and is
			 * likely a torn page.	See if the block in the double-write file
			 * has the correct checksum.  If so, we can correct the data page
			 * from the block in the double-write file.
			 */
			FileSeek(fd, DOUBLE_WRITE_HEADER_SIZE + it->slot * BLCKSZ, SEEK_SET);
			FileRead(fd, (char *) tempbuf, BLCKSZ);
			p = (PageHeader) tempbuf;
			savedCksum = p->cksum;
			p->cksum = 0;
			if (savedCksum != it->cksum ||
				savedCksum != ComputeChecksum((uint64 *) tempbuf, BLCKSZ,
											  ChecksumInit(smgr, it->forkNum,
														   it->blockNum)))
			{
				elog(LOG, "Block %s/%d has checksum error, but can't be fixed, because slot %d of double-write file %s looks invalid",
					 relpath(it->rnode, it->forkNum), it->blockNum, i, name);
			}
			else
			{
				/*
				 * Correct the block in the data file from the block in the
				 * double-write file.
				 */
				Assert(XLByteEQ(p->pd_lsn, it->pd_lsn));
				smgrwrite(smgr, it->forkNum, it->blockNum, (char *) tempbuf, false, false);
				elog(LOG, "Fixed block %s/%d (which had checksum error) from double-write file %s slot %d",
					 relpath(it->rnode, it->forkNum), it->blockNum, name, i);
			}
		}
		else
		{
			elog(DEBUG1, "Skipping slot %d of double-write file because block %s/%d is correct",
				 i, relpath(it->rnode, it->forkNum), it->blockNum);
		}
		smgrclose(smgr);
	}
	FileClose(fd);

	/*
	 * Remove double-write file, so it can't be re-applied again if crash
	 * happens during recovery.
	 */
	if (unlink(name) == -1)
	{
		elog(PANIC, "Failed to remove double-write file");
	}
	pfree(name);
	return;

badformat:
	elog(LOG, "Double-write file %s has bad format", name);
	FileClose(fd);
	pfree(name);
	return;
}

/*
 * AtEOXact_SMgr
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All transient SMgrRelation objects are closed.
 *
 * We do this as a compromise between wanting transient SMgrRelations to
 * live awhile (to amortize the costs of blind writes of multiple blocks)
 * and needing them to not live forever (since we're probably holding open
 * a kernel file descriptor for the underlying file, and we need to ensure
 * that gets closed reasonably soon if the file gets deleted).
 */
void
AtEOXact_SMgr(void)
{
	/*
	 * Zap all unowned SMgrRelations.  We rely on smgrclose() to remove each
	 * one from the list.
	 */
	while (first_unowned_reln != NULL)
	{
		Assert(first_unowned_reln->smgr_owner == NULL);
		smgrclose(first_unowned_reln);
	}
}
