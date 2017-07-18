/*-------------------------------------------------------------------------
 *
 * clog.c
 *		PostgreSQL transaction-commit-log manager
 *
 * This module replaces the old "pg_log" access code, which treated pg_log
 * essentially like a relation, in that it went through the regular buffer
 * manager.  The problem with that was that there wasn't any good way to
 * recycle storage space for transactions so old that they'll never be
 * looked up again.  Now we use specialized access code so that the commit
 * log can be broken into relatively small, independent segments.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CLOG page is initialized to zeroes.  Other writes of CLOG come from
 * recording of transaction commit or abort in xact.c, which generates its
 * own XLOG records for these events and will re-perform the status update
 * on redo; so we need make no additional XLOG entry here.  For synchronous
 * transaction commits, the XLOG is guaranteed flushed through the XLOG commit
 * record before we are called to log a commit, so the WAL rule "write xlog
 * before data" is satisfied automatically.  However, for async commits we
 * must track the latest LSN affecting each CLOG page, so that we can flush
 * XLOG that far and satisfy the WAL rule.  We don't have to worry about this
 * for aborts (whether sync or async), since the post-crash assumption would
 * be that such transactions failed anyway.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/clog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/mvccvars.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pg_trace.h"

/*
 * Defines for CLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CLOG page numbering also wraps around at 0xFFFFFFFF/CLOG_XACTS_PER_PAGE,
 * and CLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCLOG (see CLOGPagePrecedes).
 */

/* We need two bits per xact, so four xacts fit in a byte */
#define CLOG_BITS_PER_XACT	2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK	((1 << CLOG_BITS_PER_XACT) - 1)

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid)	(TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid)	((xid) % (TransactionId) CLOG_XACTS_PER_BYTE)

/* We store the latest async LSN for each group of transactions */
#define CLOG_XACTS_PER_LSN_GROUP	32	/* keep this a power of 2 */
#define CLOG_LSNS_PER_PAGE	(CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, xid)	((slotno) * CLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_LSN_GROUP)


/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData ClogCtlData;

#define ClogCtl (&ClogCtlData)


static int	ZeroCLOGPage(int pageno, bool writeXlog);
static bool CLOGPagePrecedes(int page1, int page2);
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno, TransactionId oldestXact,
					 Oid oldestXidDb);
static void CLogSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids, CLogXidStatus status,
						   XLogRecPtr lsn, int pageno);
static void CLogSetStatusBit(TransactionId xid, CLogXidStatus status,
						  XLogRecPtr lsn, int slotno);


/*
 * CLogSetTreeStatus
 *
 * Record the final state of transaction entries in the commit log for
 * a transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be
 * the top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * lsn must be the WAL location of the commit record when recording an async
 * commit.  For a synchronous commit it can be InvalidXLogRecPtr, since the
 * caller guarantees the commit record is already flushed in that case.  It
 * should be InvalidXLogRecPtr for abort cases, too.
 *
 * The atomicity is limited by whether all the subxids are in the same CLOG
 * page as xid.  If they all are, then the lock will be grabbed only once,
 * and the status will be set to committed directly.  Otherwise there is
 * a window that the parent will be seen as committed, while (some of) the
 * children are still seen as in-progress. That's OK with the current use,
 * as visibility checking code will not rely on the CLOG for recent
 * transactions (CSNLOG will be used instead).
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; functions in transam.c are the intended callers.
 *
 * XXX Think about issuing FADVISE_WILLNEED on pages that we will need,
 * but aren't yet in cache, as well as hinting pages not to fall out of
 * cache yet.
 */
void
CLogSetTreeStatus(TransactionId xid, int nsubxids,
				  TransactionId *subxids, CLogXidStatus status, XLogRecPtr lsn)
{
	TransactionId topXid;
	int			pageno;
	int			i;
	int			offset;

	Assert(status == CLOG_XID_STATUS_COMMITTED ||
		   status == CLOG_XID_STATUS_ABORTED);

	/*
	 * Update the clog page-by-page. On first iteration, we will set the
	 * status of the top-XID, and any subtransactions on the same page.
	 */
	pageno = TransactionIdToPage(xid);		/* get page of parent */
	topXid = xid;
	offset = 0;
	i = 0;
	for (;;)
	{
		int			num_on_page = 0;

		while (i < nsubxids && TransactionIdToPage(subxids[i]) == pageno)
		{
			num_on_page++;
			i++;
		}

		CLogSetPageStatus(topXid,
						  num_on_page, subxids + offset,
						  status, lsn, pageno);

		if (i == nsubxids)
			break;

		offset = i;
		pageno = TransactionIdToPage(subxids[offset]);
		topXid = InvalidTransactionId;
	}
}

/*
 * Record the final state of transaction entries in the commit log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as CLogSetTreeStatus()
 */
static void
CLogSetPageStatus(TransactionId xid, int nsubxids,
				  TransactionId *subxids, CLogXidStatus status,
				  XLogRecPtr lsn, int pageno)
{
	int			slotno;
	int			i;

	Assert(status == CLOG_XID_STATUS_COMMITTED ||
		   status == CLOG_XID_STATUS_ABORTED);

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/*
	 * If we're doing an async commit (ie, lsn is valid), then we must wait
	 * for any active write on the page slot to complete.  Otherwise our
	 * update could reach disk in that write, which will not do since we
	 * mustn't let it reach disk until we've done the appropriate WAL flush.
	 * But when lsn is invalid, it's OK to scribble on a page while it is
	 * write-busy, since we don't care if the update reaches disk sooner than
	 * we think.
	 */
	slotno = SimpleLruReadPage(ClogCtl, pageno, XLogRecPtrIsInvalid(lsn), xid);

	/* Set the main transaction id, if any. */
	if (TransactionIdIsValid(xid))
		CLogSetStatusBit(xid, status, lsn, slotno);

	/* Set the subtransactions */
	for (i = 0; i < nsubxids; i++)
	{
		Assert(ClogCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CLogSetStatusBit(subxids[i], status, lsn, slotno);
	}

	ClogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CLogControlLock);
}

/*
 * Sets the commit status of a single transaction.
 *
 * Must be called with CLogControlLock held
 */
static void
CLogSetStatusBit(TransactionId xid, CLogXidStatus status, XLogRecPtr lsn, int slotno)
{
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	char	   *byteptr;
	char		byteval;
	char		curval;

	byteptr = ClogCtl->shared->page_buffer[slotno] + byteno;
	curval = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

	/*
	 * Current state change should be from 0 or subcommitted to target state
	 * or we should already be there when replaying changes during recovery.
	 */
	Assert(curval == 0 ||
		   (curval == CLOG_XID_STATUS_SUB_COMMITTED &&
			status != CLOG_XID_STATUS_IN_PROGRESS) ||
		   curval == status);

	/* note this assumes exclusive access to the clog page */
	byteval = *byteptr;
	byteval &= ~(((1 << CLOG_BITS_PER_XACT) - 1) << bshift);
	byteval |= (status << bshift);
	*byteptr = byteval;

	/*
	 * Update the group LSN if the transaction completion LSN is higher.
	 *
	 * Note: lsn will be invalid when supplied during InRecovery processing,
	 * so we don't need to do anything special to avoid LSN updates during
	 * recovery. After recovery completes the next clog change will set the
	 * LSN correctly.
	 */
	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			lsnindex = GetLSNIndex(slotno, xid);

		if (ClogCtl->shared->group_lsn[lsnindex] < lsn)
			ClogCtl->shared->group_lsn[lsnindex] = lsn;
	}
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
CLogXidStatus
CLogGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	int			pageno = TransactionIdToPage(xid);
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	int			slotno;
	int			lsnindex;
	char	   *byteptr;
	CLogXidStatus	status;

	/* lock is acquired by SimpleLruReadPage_ReadOnly */

	slotno = SimpleLruReadPage_ReadOnly(ClogCtl, pageno, xid);
	byteptr = ClogCtl->shared->page_buffer[slotno] + byteno;

	status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

	lsnindex = GetLSNIndex(slotno, xid);
	*lsn = ClogCtl->shared->group_lsn[lsnindex];

	LWLockRelease(CLogControlLock);

	return status;
}

/*
 * Number of shared CLOG buffers.
 *
 * On larger multi-processor systems, it is possible to have many CLOG page
 * requests in flight at one time which could lead to disk access for CLOG
 * page if the required page is not found in memory.  Testing revealed that we
 * can get the best performance by having 128 CLOG buffers, more than that it
 * doesn't improve performance.
 *
 * Unconditionally keeping the number of CLOG buffers to 128 did not seem like
 * a good idea, because it would increase the minimum amount of shared memory
 * required to start, which could be a problem for people running very small
 * configurations.  The following formula seems to represent a reasonable
 * compromise: people with very low values for shared_buffers will get fewer
 * CLOG buffers as well, and everyone else will get 128.
 */
Size
CLOGShmemBuffers(void)
{
	return Min(128, Max(4, NBuffers / 512));
}

/*
 * Initialization of shared memory for CLOG
 */
Size
CLOGShmemSize(void)
{
	return SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE);
}

void
CLOGShmemInit(void)
{
	ClogCtl->PagePrecedes = CLOGPagePrecedes;
	SimpleLruInit(ClogCtl, "clog", CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE,
				  CLogControlLock, "pg_xact", LWTRANCHE_CLOG_BUFFERS);
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CLOG segment.  (The CLOG directory is assumed to
 * have been created by initdb, and CLOGShmemInit must have been
 * called already.)
 */
void
BootStrapCLOG(void)
{
	int			slotno;

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCLOGPage(0, false);

	/* Make sure it's written out */
	SimpleLruWritePage(ClogCtl, slotno);
	Assert(!ClogCtl->shared->page_dirty[slotno]);

	LWLockRelease(CLogControlLock);
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
ZeroCLOGPage(int pageno, bool writeXlog)
{
	int			slotno;

	slotno = SimpleLruZeroPage(ClogCtl, pageno);

	if (writeXlog)
		WriteZeroPageXlogRec(pageno);

	return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 */
void
StartupCLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/*
	 * Initialize our idea of the latest page number.
	 */
	ClogCtl->shared->latest_page_number = pageno;

	LWLockRelease(CLogControlLock);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/*
	 * Re-Initialize our idea of the latest page number.
	 */
	ClogCtl->shared->latest_page_number = pageno;

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
		int			byteno = TransactionIdToByte(xid);
		int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
		int			slotno;
		char	   *byteptr;

		slotno = SimpleLruReadPage(ClogCtl, pageno, false, xid);
		byteptr = ClogCtl->shared->page_buffer[slotno] + byteno;

		/* Zero so-far-unused positions in the current byte */
		*byteptr &= (1 << bshift) - 1;
		/* Zero the rest of the page */
		MemSet(byteptr + 1, 0, BLCKSZ - byteno - 1);

		ClogCtl->shared->page_dirty[slotno] = true;
	}

	LWLockRelease(CLogControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCLOG(void)
{
	/* Flush dirty CLOG pages to disk */
	TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(false);
	SimpleLruFlush(ClogCtl, false);

	/*
	 * fsync pg_xact to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_xact", true);

	TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(false);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCLOG(void)
{
	/* Flush dirty CLOG pages to disk */
	TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(true);
	SimpleLruFlush(ClogCtl, true);

	/*
	 * fsync pg_xact to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_xact", true);

	TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(true);
}


/*
 * Make sure that CLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCLOG(TransactionId newestXact)
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

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCLOGPage(pageno, true);

	LWLockRelease(CLogControlLock);
}


/*
 * Remove all CLOG segments before the one holding the passed transaction ID
 *
 * Before removing any CLOG data, we must flush XLOG to disk, to ensure
 * that any recently-emitted HEAP_FREEZE records have reached disk; otherwise
 * a crash and restart might leave us with some unfrozen tuples referencing
 * removed CLOG data.  We choose to emit a special TRUNCATE XLOG record too.
 * Replaying the deletion from XLOG is not critical, since the files could
 * just as well be removed later, but doing so prevents a long-running hot
 * standby server from acquiring an unreasonably bloated CLOG directory.
 *
 * Since CLOG segments hold a large number of transactions, the opportunity to
 * actually remove a segment is fairly rare, and so it seems best not to do
 * the XLOG flush unless we have confirmed that there is a removable segment.
 */
void
TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!SlruScanDirectory(ClogCtl, SlruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

	/*
	 * Advance oldestClogXid before truncating clog, so concurrent xact status
	 * lookups can ensure they don't attempt to access truncated-away clog.
	 *
	 * It's only necessary to do this if we will actually truncate away clog
	 * pages.
	 */
	AdvanceOldestClogXid(oldestXact);

	/* vac_truncate_clog already advanced oldestXid */
	Assert(TransactionIdPrecedesOrEquals(oldestXact,
										 ShmemVariableCache->oldestXid));

	/*
	 * Write XLOG record and flush XLOG to disk. We record the oldest xid
	 * we're keeping information about here so we can ensure that it's always
	 * ahead of clog truncation in case we crash, and so a standby finds out
	 * the new valid xid before the next checkpoint.
	 */
	WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid);

	/* Now we can remove the old CLOG segment(s) */
	SimpleLruTruncate(ClogCtl, cutoffPage);
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
CLOGPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}


/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroPageXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	(void) XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes
 * in TruncateCLOG().
 */
static void
WriteTruncateXlogRec(int pageno, TransactionId oldestXact, Oid oldestXactDb)
{
	XLogRecPtr	recptr;
	xl_clog_truncate xlrec;

	xlrec.pageno = pageno;
	xlrec.oldestXact = oldestXact;
	xlrec.oldestXactDb = oldestXactDb;

	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), sizeof(xl_clog_truncate));
	recptr = XLogInsert(RM_CLOG_ID, CLOG_TRUNCATE);
	XLogFlush(recptr);
}

/*
 * CLOG resource manager's routines
 */
void
clog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in clog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == CLOG_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

		slotno = ZeroCLOGPage(pageno, false);
		SimpleLruWritePage(ClogCtl, slotno);
		Assert(!ClogCtl->shared->page_dirty[slotno]);

		LWLockRelease(CLogControlLock);
	}
	else if (info == CLOG_TRUNCATE)
	{
		xl_clog_truncate xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_clog_truncate));

		/*
		 * During XLOG replay, latest_page_number isn't set up yet; insert a
		 * suitable value to bypass the sanity test in SimpleLruTruncate.
		 */
		ClogCtl->shared->latest_page_number = xlrec.pageno;

		AdvanceOldestClogXid(xlrec.oldestXact);

		SimpleLruTruncate(ClogCtl, xlrec.pageno);
	}
	else
		elog(PANIC, "clog_redo: unknown op code %u", info);
}
