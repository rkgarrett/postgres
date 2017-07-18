/*-------------------------------------------------------------------------
 *
 * transam.c
 *	  postgres transaction (commit) log interface routines
 *
 * This module contains high level functions for managing the status
 * of transactions. It sits on top of two lower level structures: the
 * CLOG, and the CSNLOG. The CLOG is a permanent on-disk structure that
 * tracks the committed/aborted status for each transaction ID. The CSNLOG
 * tracks *when* each transaction ID committed (or aborted). The CSNLOG
 * is used when checking the status of recent transactions that might still
 * be in-progress, and it is reset at server startup. The CLOG is used for
 * older transactions that are known to have completed (or crashed).
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/transam.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/mvccvars.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "storage/lmgr.h"
#include "utils/snapmgr.h"

/*
 * Single-item cache for results of TransactionIdGetCommitSeqNo.  It's worth
 * having
 * such a cache because we frequently find ourselves repeatedly checking the
 * same XID, for example when scanning a table just after a bulk insert,
 * update, or delete.
 */
static TransactionId cachedFetchXid = InvalidTransactionId;
static CommitSeqNo cachedCSN;

/*
 * Also have a (separate) cache for CLogGetCommitLSN()
 */
static TransactionId cachedLSNFetchXid = InvalidTransactionId;
static XLogRecPtr cachedCommitLSN;

/*
 * TransactionIdGetCommitSeqNo --- fetch CSN of specified transaction id
 */
CommitSeqNo
TransactionIdGetCommitSeqNo(TransactionId transactionId)
{
	CommitSeqNo	csn;

	/*
	 * Before going to the commit log manager, check our single item cache to
	 * see if we didn't just check the transaction status a moment ago.
	 */
	if (TransactionIdEquals(transactionId, cachedFetchXid))
		return cachedCSN;

	/*
	 * Also, check to see if the transaction ID is a permanent one.
	 */
	if (!TransactionIdIsNormal(transactionId))
	{
		if (TransactionIdEquals(transactionId, BootstrapTransactionId))
			return COMMITSEQNO_FROZEN;
		if (TransactionIdEquals(transactionId, FrozenTransactionId))
			return COMMITSEQNO_FROZEN;
		return COMMITSEQNO_ABORTED;
	}

	/*
	 * If the XID is older than TransactionXmin, check the clog. Otherwise
	 * check the csnlog.
	 */
	Assert(TransactionIdIsValid(TransactionXmin));
	if (TransactionIdPrecedes(transactionId, TransactionXmin))
	{
		XLogRecPtr lsn;

		if (CLogGetStatus(transactionId, &lsn) == CLOG_XID_STATUS_COMMITTED)
			csn = COMMITSEQNO_FROZEN;
		else
			csn = COMMITSEQNO_ABORTED;
	}
	else
	{
		csn = CSNLogGetCommitSeqNo(transactionId);

		if (csn == COMMITSEQNO_COMMITTING)
		{
			/*
			 * If the transaction is committing at this very instant, and
			 * hasn't set its CSN yet, wait for it to finish doing so.
			 *
			 * XXX: Alternatively, we could wait on the heavy-weight lock on
			 * the XID. that'd make TransactionIdCommitTree() slightly
			 * cheaper, as it wouldn't need to acquire CommitSeqNoLock (even
			 * in shared mode).
			 */
			LWLockAcquire(CommitSeqNoLock, LW_EXCLUSIVE);
			LWLockRelease(CommitSeqNoLock);

			csn = CSNLogGetCommitSeqNo(transactionId);
			Assert(csn != COMMITSEQNO_COMMITTING);
		}
	}

	/*
	 * Cache it, but DO NOT cache status for unfinished transactions!
	 * We only cache status that is guaranteed not to change.
	 */
	if (COMMITSEQNO_IS_COMMITTED(csn) ||
		COMMITSEQNO_IS_ABORTED(csn))
	{
		cachedFetchXid = transactionId;
		cachedCSN = csn;
	}

	return csn;
}

/*
 * TransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
bool							/* true if given transaction committed */
TransactionIdDidCommit(TransactionId transactionId)
{
	CommitSeqNo csn;

	csn = TransactionIdGetCommitSeqNo(transactionId);

	if (COMMITSEQNO_IS_COMMITTED(csn))
		return true;
	else
		return false;
}

/*
 * TransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
bool							/* true if given transaction aborted */
TransactionIdDidAbort(TransactionId transactionId)
{
	CommitSeqNo csn;

	csn = TransactionIdGetCommitSeqNo(transactionId);

	if (COMMITSEQNO_IS_ABORTED(csn))
		return true;
	else
		return false;
}

/*
 * TransactionIdGetStatus
 *		Returns the status of the transaction.
 *
 * Note that this treats a a crashed transaction as still in-progress,
 * until it falls off the xmin horizon.
 */
TransactionIdStatus
TransactionIdGetStatus(TransactionId xid)
{
	CommitSeqNo csn;

	csn = TransactionIdGetCommitSeqNo(xid);

	if (COMMITSEQNO_IS_COMMITTED(csn))
		return XID_COMMITTED;
	else if (COMMITSEQNO_IS_ABORTED(csn))
		return XID_ABORTED;
	else
		return XID_INPROGRESS;
}

/*
 * TransactionIdCommitTree
 *		Marks the given transaction and children as committed
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 */
void
TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids)
{
	TransactionIdAsyncCommitTree(xid, nxids, xids, InvalidXLogRecPtr);
}

/*
 * TransactionIdAsyncCommitTree
 *		Same as above, but for async commits.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 */
void
TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids,
							 XLogRecPtr lsn)
{
	CommitSeqNo csn;
	TransactionId latestXid;
	TransactionId currentLatestCompletedXid;

	latestXid = TransactionIdLatest(xid, nxids, xids);

	/*
	 * Grab the CommitSeqNoLock, in shared mode. This is only used to
	 * provide a way for a concurrent transaction to wait for us to
	 * complete (see TransactionIdGetCommitSeqNo()).
	 *
	 * XXX: We could reduce the time the lock is held, by only setting
	 * the CSN on the top-XID while holding the lock, and updating the
	 * sub-XIDs later. But it doesn't matter much, because we're only
	 * holding it in shared mode, and it's rare for it to be acquired
	 * in exclusive mode.
	 */
	LWLockAcquire(CommitSeqNoLock, LW_SHARED);

	/*
	 * First update latestCompletedXid to cover this xid. We do this before
	 * assigning a CSN, so that if someone acquires a new snapshot at the same
	 * time, the xmax it computes is sure to cover our XID.
	 */
	currentLatestCompletedXid = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
	while (TransactionIdFollows(latestXid, currentLatestCompletedXid))
	{
		if (pg_atomic_compare_exchange_u32(&ShmemVariableCache->latestCompletedXid,
										   &currentLatestCompletedXid,
										   latestXid))
			break;
	}

	/*
	 * Mark our top transaction id as commit-in-progress.
	 */
	CSNLogSetCommitSeqNo(xid, 0, NULL, COMMITSEQNO_COMMITTING);

	/* Get our CSN and increment */
	csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
	Assert(csn >= COMMITSEQNO_FIRST_NORMAL);

	/* Stamp this XID (and sub-XIDs) with the CSN */
	CSNLogSetCommitSeqNo(xid, nxids, xids, csn);

	LWLockRelease(CommitSeqNoLock);

	/*
	 * Also update the CLOG. This doesn't need to happen atomically with
	 * updating the CSN log, because no-one will look at the CLOG until
	 * GlobalXmin has advanced past our XID, and that can't happen until
	 * we clear the XID from the proc array.
	 */
	CLogSetTreeStatus(xid, nxids, xids,
					  CLOG_XID_STATUS_COMMITTED,
					  lsn);
}

/*
 * TransactionIdAbortTree
 *		Marks the given transaction and children as aborted.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * We don't need to worry about the non-atomic behavior, since any onlookers
 * will consider all the xacts as not-yet-committed anyway.
 */
void
TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids)
{
	TransactionId latestXid;
	TransactionId currentLatestCompletedXid;

	latestXid = TransactionIdLatest(xid, nxids, xids);

	currentLatestCompletedXid = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
	while (TransactionIdFollows(latestXid, currentLatestCompletedXid))
	{
		if (pg_atomic_compare_exchange_u32(&ShmemVariableCache->latestCompletedXid,
										   &currentLatestCompletedXid,
										   latestXid))
			break;
	}

	CSNLogSetCommitSeqNo(xid, nxids, xids, COMMITSEQNO_ABORTED);
	CLogSetTreeStatus(xid, nxids, xids,
					  CLOG_XID_STATUS_ABORTED, InvalidCommitSeqNo);
}

/*
 * TransactionIdPrecedes --- is id1 logically < id2?
 */
bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.  If both are normal, do a modulo-2^32 comparison.
	 */
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}

/*
 * TransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
bool
TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 <= id2);

	diff = (int32) (id1 - id2);
	return (diff <= 0);
}

/*
 * TransactionIdFollows --- is id1 logically > id2?
 */
bool
TransactionIdFollows(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 > id2);

	diff = (int32) (id1 - id2);
	return (diff > 0);
}

/*
 * TransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
bool
TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 >= id2);

	diff = (int32) (id1 - id2);
	return (diff >= 0);
}


/*
 * TransactionIdLatest --- get latest XID among a main xact and its children
 */
TransactionId
TransactionIdLatest(TransactionId mainxid,
					int nxids, const TransactionId *xids)
{
	TransactionId result;

	/*
	 * In practice it is highly likely that the xids[] array is sorted, and so
	 * we could save some cycles by just taking the last child XID, but this
	 * probably isn't so performance-critical that it's worth depending on
	 * that assumption.  But just to show we're not totally stupid, scan the
	 * array back-to-front to avoid useless assignments.
	 */
	result = mainxid;
	while (--nxids >= 0)
	{
		if (TransactionIdPrecedes(result, xids[nxids]))
			result = xids[nxids];
	}
	return result;
}


/*
 * TransactionIdGetCommitLSN
 *
 * This function returns an LSN that is late enough to be able
 * to guarantee that if we flush up to the LSN returned then we
 * will have flushed the transaction's commit record to disk.
 *
 * The result is not necessarily the exact LSN of the transaction's
 * commit record!  For example, for long-past transactions (those whose
 * clog pages already migrated to disk), we'll return InvalidXLogRecPtr.
 * Also, because we group transactions on the same clog page to conserve
 * storage, we might return the LSN of a later transaction that falls into
 * the same group.
 */
XLogRecPtr
TransactionIdGetCommitLSN(TransactionId xid)
{
	XLogRecPtr	result;

	/*
	 * Currently, all uses of this function are for xids that were just
	 * reported to be committed by TransactionLogFetch, so we expect that
	 * checking TransactionLogFetch's cache will usually succeed and avoid an
	 * extra trip to shared memory.
	 */
	if (TransactionIdEquals(xid, cachedLSNFetchXid))
		return cachedCommitLSN;

	/* Special XIDs are always known committed */
	if (!TransactionIdIsNormal(xid))
		return InvalidXLogRecPtr;

	/*
	 * Get the transaction status.
	 */
	(void) CLogGetStatus(xid, &result);

	cachedLSNFetchXid = xid;
	cachedCommitLSN = result;

	return result;
}
