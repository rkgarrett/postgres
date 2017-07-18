/*
 * csnlog.h
 *
 * Commit-Sequence-Number log.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clog.h
 */
#ifndef CSNLOG_H
#define CSNLOG_H

#include "access/xlog.h"

extern void CSNLogSetCommitSeqNo(TransactionId xid, int nsubxids,
					 TransactionId *subxids, CommitSeqNo csn);
extern CommitSeqNo CSNLogGetCommitSeqNo(TransactionId xid);

extern Size CSNLOGShmemBuffers(void);
extern Size CSNLOGShmemSize(void);
extern void CSNLOGShmemInit(void);
extern void BootStrapCSNLOG(void);
extern void StartupCSNLOG(TransactionId oldestActiveXID);
extern void TrimCSNLOG(void);
extern void ShutdownCSNLOG(void);
extern void CheckPointCSNLOG(void);
extern void ExtendCSNLOG(TransactionId newestXact);
extern void TruncateCSNLOG(TransactionId oldestXact);

#endif   /* CSNLOG_H */
