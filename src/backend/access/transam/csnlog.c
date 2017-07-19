/*-------------------------------------------------------------------------
 *
 * csnlog.c
 *		Tracking Commit-Sequence-Numbers and in-progress subtransactions
 *
 * The pg_csnlog manager is a pg_clog-like manager that stores the commit
 * sequence number, or parent transaction Id, for each transaction.  It is
 * a fundamental part of MVCC.
 *
 * The csnlog serves two purposes:
 *
 * 1. While a transaction is in progress, it stores the parent transaction
 * Id for each in-progress subtransaction. A main transaction has a parent
 * of InvalidTransactionId, and each subtransaction has its immediate
 * parent. The tree can easily be walked from child to parent, but not in
 * the opposite direction.
 *
 * 2. After a transaction has committed, it stores the Commit Sequence
 * Number of the commit.
 *
 * We can use the same structure for both, because we don't care about the
 * parent-child relationships subtransaction after commit.
 *
 * This code is based on clog.c, but the robustness requirements
 * are completely different from pg_clog, because we only need to remember
 * pg_csnlog information for currently-open and recently committed
 * transactions.  Thus, there is no need to preserve data over a crash and
 * restart.
 *
 * There are no XLOG interactions since we do not care about preserving
 * data across crashes.  During database startup, we simply force the
 * currently-active page of CSNLOG to zeroes.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csnlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csnlog.h"
#include "access/mvccvars.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"

/*
 * Defines for CSNLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLOG page numbering also wraps around at 0xFFFFFFFF/CSNLOG_XACTS_PER_PAGE,
 * and CSNLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLOG (see CSNLOGPagePrecedes).
 */

/* We store the commit LSN for each xid */
#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CSNLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CSNLOG_XACTS_PER_PAGE)

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CsnlogCtlData;

#define CsnlogCtl (&CsnlogCtlData)


static int	ZeroCSNLOGPage(int pageno);
static bool CSNLOGPagePrecedes(int page1, int page2);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids,
						   CommitSeqNo csn, int pageno);
static void CSNLogSetCSN(TransactionId xid, CommitSeqNo csn, int slotno);

/*
 * CSNLogSetCommitSeqNo
 *
 * Record the status and CSN of transaction entries in the commit log for a
 * transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be the
 * top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * InvalidCommitSeqNo for abort cases.
 *
 * Note: This doesn't guarantee atomicity. The caller can use the
 * COMMITSEQNO_COMMITTING special value for that.
 */
void
CSNLogSetCommitSeqNo(TransactionId xid, int nsubxids,
					 TransactionId *subxids, CommitSeqNo csn)
{
	int			pageno;
	int			i = 0;
	int			offset = 0;

	if (csn == InvalidCommitSeqNo || xid == BootstrapTransactionId)
	{
		if (IsBootstrapProcessingMode())
			csn = COMMITSEQNO_FROZEN;
		else
			elog(ERROR, "cannot mark transaction committed without CSN");
	}

	pageno = TransactionIdToPage(xid);		/* get page of parent */
	for (;;)
	{
		int			num_on_page = 0;

		while (i < nsubxids && TransactionIdToPage(subxids[i]) == pageno)
		{
			num_on_page++;
			i++;
		}

		CSNLogSetPageStatus(xid,
							num_on_page, subxids + offset,
							csn, pageno);
		if (i >= nsubxids)
			break;

		offset = i;
		pageno = TransactionIdToPage(subxids[offset]);
		xid = InvalidTransactionId;
	}
}

/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CSNLogSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids,
						   CommitSeqNo csn, int pageno)
{
	int			slotno;
	int			i;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CsnlogCtl, pageno, true, xid);

	/* Subtransactions first, if needed ... */
	for (i = 0; i < nsubxids; i++)
	{
		Assert(CsnlogCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CSNLogSetCSN(subxids[i],	csn, slotno);
	}

	/* ... then the main transaction */
	if (TransactionIdIsValid(xid))
		CSNLogSetCSN(xid, csn, slotno);

	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CSNLogControlLock);
}



/*
 * Record the parent of a subtransaction in the subtrans log.
 */
void
SubTransSetParent(TransactionId xid, TransactionId parent)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToPgIndex(xid);
	int			slotno;
	CommitSeqNo *ptr;
	CommitSeqNo newcsn;

	Assert(TransactionIdIsValid(parent));
	Assert(TransactionIdFollows(xid, parent));

	newcsn = CSN_SUBTRANS_BIT | (uint64) parent;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CsnlogCtl, pageno, true, xid);
	ptr = (CommitSeqNo *) CsnlogCtl->shared->page_buffer[slotno];
	ptr += entryno;

	/*
	 * It's possible we'll try to set the parent xid multiple times but we
	 * shouldn't ever be changing the xid from one valid xid to another valid
	 * xid, which would corrupt the data structure.
	 */
	if (*ptr != newcsn)
	{
		Assert(*ptr == COMMITSEQNO_INPROGRESS);
		*ptr = newcsn;
		SubTransCtl->shared->page_dirty[slotno] = true;
	}

	LWLockRelease(CSNLogControlLock);
}

/*
 * Interrogate the parent of a transaction in the csnlog.
 */
TransactionId
SubTransGetParent(TransactionId xid)
{
	CommitSeqNo csn;

	csn = CSNLogGetCommitSeqNo(xid);

	if (COMMITSEQNO_IS_SUBTRANS(csn))
		return (TransactionId) (csn & 0xFFFFFFFF);
	else
		return InvalidTransactionId;
}

/*
 * SubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 */
TransactionId
SubTransGetTopmostTransaction(TransactionId xid)
{
	TransactionId parentXid = xid,
				previousXid = xid;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	while (TransactionIdIsValid(parentXid))
	{
		previousXid = parentXid;
		if (TransactionIdPrecedes(parentXid, TransactionXmin))
			break;
		parentXid = SubTransGetParent(parentXid);
	}

	Assert(TransactionIdIsValid(previousXid));

	return previousXid;
}




/*
 * Sets the commit status of a single transaction.
 *
 * Must be called with CSNLogControlLock held
 */
static void
CSNLogSetCSN(TransactionId xid, CommitSeqNo csn, int slotno)
{
	int			entryno = TransactionIdToPgIndex(xid);
	CommitSeqNo *ptr;

	ptr = (CommitSeqNo *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

	/*
	 * Current state change should be from 0 to target state. (Allow
	 * setting it again to same value.)
	 */
	Assert(COMMITSEQNO_IS_INPROGRESS(*ptr) ||
		   COMMITSEQNO_IS_COMMITTING(*ptr) ||
		   COMMITSEQNO_IS_SUBTRANS(*ptr) ||
		   *ptr == csn);

	*ptr = csn;
}

/*
 * Interrogate the state of a transaction in the commit log.
 *
 * Aside from the actual commit status, this function returns (into *lsn)
 * an LSN that is late enough to be able to guarantee that if we flush up to
 * that LSN then we will have flushed the transaction's commit record to disk.
 * The result is not necessarily the exact LSN of the transaction's commit
 * record!	For example, for long-past transactions (those whose clog pages
 * already migrated to disk), we'll return InvalidXLogRecPtr.  Also, because
 * we group transactions on the same clog page to conserve storage, we might
 * return the LSN of a later transaction that falls into the same group.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionLogFetch() in transam.c is the intended caller.
 */
CommitSeqNo
CSNLogGetCommitSeqNo(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToPgIndex(xid);
	int			slotno;
	XLogRecPtr *ptr;
	XLogRecPtr	commitlsn;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	if (!TransactionIdIsNormal(xid))
	{
		if (xid == InvalidTransactionId)
			return COMMITSEQNO_ABORTED;
		if (xid == FrozenTransactionId || xid == BootstrapTransactionId)
			return COMMITSEQNO_FROZEN;
	}

	/* lock is acquired by SimpleLruReadPage_ReadOnly */

	slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl, pageno, xid);
	ptr = (XLogRecPtr *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

	commitlsn = *ptr;

	LWLockRelease(CSNLogControlLock);

	return commitlsn;
}

/*
 * Number of shared CSNLOG buffers.
 */
Size
CSNLOGShmemBuffers(void)
{
	return Min(32, Max(4, NBuffers / 512));
}

/*
 * Initialization of shared memory for CSNLOG
 */
Size
CSNLOGShmemSize(void)
{
	return SimpleLruShmemSize(CSNLOGShmemBuffers(), 0);
}

void
CSNLOGShmemInit(void)
{
	CsnlogCtl->PagePrecedes = CSNLOGPagePrecedes;
	SimpleLruInit(CsnlogCtl, "CSNLOG Ctl", CSNLOGShmemBuffers(), 0,
				  CSNLogControlLock, "pg_csnlog", LWTRANCHE_CSNLOG_BUFFERS);
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CSNLOG segment.  (The pg_csnlog directory is assumed to
 * have been created by initdb, and CSNLOGShmemInit must have been
 * called already.)
 */
void
BootStrapCSNLOG(void)
{
	int			slotno;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCSNLOGPage(0);

	/* Make sure it's written out */
	SimpleLruWritePage(CsnlogCtl, slotno);
	Assert(!CsnlogCtl->shared->page_dirty[slotno]);

	LWLockRelease(CSNLogControlLock);
}

/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCSNLOGPage(int pageno)
{
	return SimpleLruZeroPage(CsnlogCtl, pageno);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupCSNLOG(TransactionId oldestActiveXID)
{
	int			startPage;
	int			endPage;

	/*
	 * Since we don't expect pg_csnlog to be valid across crashes, we
	 * initialize the currently-active page(s) to zeroes during startup.
	 * Whenever we advance into a new page, ExtendCSNLOG will likewise zero
	 * the new page without regard to whatever was previously on disk.
	 */
	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	startPage = TransactionIdToPage(oldestActiveXID);
	endPage = TransactionIdToPage(ShmemVariableCache->nextXid);

	while (startPage != endPage)
	{
		(void) ZeroCSNLOGPage(startPage);
		startPage++;
		/* must account for wraparound */
		if (startPage > TransactionIdToPage(MaxTransactionId))
			startPage = 0;
	}
	(void) ZeroCSNLOGPage(startPage);

	LWLockRelease(CSNLogControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCSNLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely as a debugging aid.
	 */
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(false);
	SimpleLruFlush(CsnlogCtl, false);
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(false);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCSNLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/*
	 * Re-Initialize our idea of the latest page number.
	 */
	CsnlogCtl->shared->latest_page_number = pageno;

	/*
	 * Zero out the remainder of the current clog page.  Under normal
	 * circumstances it should be zeroes already, but it seems at least
	 * theoretically possible that XLOG replay will have settled on a nextXID
	 * value that is less than the last XID actually used and marked by the
	 * previous database lifecycle (since subtransaction commit writes clog
	 * but makes no WAL entry).  Let's just be safe. (We need not worry about
	 * pages beyond the current one, since those will be zeroed when first
	 * used.  For the same reason, there is no need to do anything when
	 * nextXid is exactly at a page boundary; and it's likely that the
	 * "current" page doesn't exist yet in that case.)
	 */
	if (TransactionIdToPgIndex(xid) != 0)
	{
		int			entryno = TransactionIdToPgIndex(xid);
		int			byteno = entryno * sizeof(XLogRecPtr);
		int			slotno;
		char	   *byteptr;

		slotno = SimpleLruReadPage(CsnlogCtl, pageno, false, xid);

		byteptr = CsnlogCtl->shared->page_buffer[slotno] + byteno;

		/* Zero the rest of the page */
		MemSet(byteptr, 0, BLCKSZ - byteno);

		CsnlogCtl->shared->page_dirty[slotno] = true;
	}

	LWLockRelease(CSNLogControlLock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCSNLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(true);
	SimpleLruFlush(CsnlogCtl, true);
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(true);
}


/*
 * Make sure that CSNLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCSNLOG(TransactionId newestXact)
{
	int			pageno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCSNLOGPage(pageno);

	LWLockRelease(CSNLogControlLock);
}


/*
 * Remove all CSNLOG segments before the one holding the passed transaction ID
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCSNLOG(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	SimpleLruTruncate(CsnlogCtl, cutoffPage);
}


/*
 * Decide which of two CLOG page numbers is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CSNLOGPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CSNLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CSNLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}
