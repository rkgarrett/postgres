/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "fmgr.h"
#include "fd.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "access/xlogdefs.h"


/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrRelation is
 * closed.  We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 *
 * SMgrRelations that do not have an "owner" are considered to be transient,
 * and are deleted at end of transaction.
 */
typedef struct SMgrRelationData
{
	/* rnode is the hashtable lookup key, so it must be first! */
	RelFileNodeBackend smgr_rnode;	/* relation physical identifier */

	/* pointer to owning pointer, or NULL if none */
	struct SMgrRelationData **smgr_owner;

	/*
	 * These next three fields are not actually used or manipulated by smgr,
	 * except that they are reset to InvalidBlockNumber upon a cache flush
	 * event (in particular, upon truncation of the relation).  Higher levels
	 * store cached state here so that it will be reset when truncation
	 * happens.  In all three cases, InvalidBlockNumber means "unknown".
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_fsm_nblocks;	/* last known size of fsm fork */
	BlockNumber smgr_vm_nblocks;	/* last known size of vm fork */

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	int			smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs[MAX_FORKNUM + 1];
	struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

	/* if unowned, list link in list of all unowned SMgrRelations */
	struct SMgrRelationData *next_unowned_reln;
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

#define SmgrIsTemp(smgr) \
	RelFileNodeBackendIsTemp((smgr)->smgr_rnode)

/* Number of blocks in the double-write buffer */
#define MAX_DW_BLOCKS		128
#define CKPT_DW_BLOCKS		64
#define NON_CKPT_DW_BLOCKS	128

/* Maximum number of blocks in a batch written all at once. */
/* XXX How to make sure we can cache all needed fds in Vfd cache */
#define MAX_BATCH_BLOCKS 64

/* Maximum number of batches per double-write buffer */
#define MAX_BATCHES_PER_DWBUF  8

/*
 * Element of an array specifying a buffer write in a list of buffer
 * writes being batched.
 */
struct SMgrWriteList
{
	RelFileNodeBackend smgr_rnode;
	ForkNumber	forkNum;
	BlockNumber blockNum;
	int			slot;
	char	   *buffer;

	SMgrRelation localrel;

	/* Filled in by mdbwrite */
	File		fd;
	off_t		seekPos;
	int			len;
};

#include "storage/lwlock.h"

struct DWBufferInfo
{
	char *bufferStart;
	/* length of this double-write buffer in blocks, power of 2 */
	int length;
	/* mask for modding index to circular buffer, equals (length-1) */
	int mask;

	/*
	 * start, endAlloc, endFill are monotonically increasing, must
	 * alwayed be modded by MAX_DW_BLOCKS to access writeList.
	 */
	/* Start of allocated entries in writeList, advanced as batches are
	 * flushed out. */
	volatile int start;
	/* End of allocated entries */
	volatile int endAlloc;
	/* End of entries that have been filled in */
	volatile int endFill;

	/*
	 * Lock protecting writeList and start/endFill/flushed
	 */
	LWLockId writeLock;
	LWLockId allocLock;
	/* Has this batch been flushed? */
	int flushed[MAX_DW_BLOCKS];
	/* Lock per batch */
	LWLockId batchLocks[MAX_DW_BLOCKS];
	struct SMgrWriteList writeList[MAX_DW_BLOCKS];

	int readHits;
	int fullWaits;
};

/*
 * One double-write buffer for all processes except the checkpointer, and
 * one double-write buffer for the checkpointer.
 */
#define DWBUF_NON_CHECKPOINTER 0
#define DWBUF_CHECKPOINTER 1

/*
 * Description of one buffer in the double-write file, as listed in the
 * header.
 */
struct DoubleBufItem
{
	/* Specification of where this buffer is in the database */
	RelFileNodeBackend rnode;	/* physical relation identifier */
	ForkNumber	forkNum;
	BlockNumber blockNum;		/* blknum relative to begin of reln */
	int			slot;			/* Slot where buffer is stored */

	/* Checksum of the buffer. */
	int32		cksum;
	/* LSN of the buffer */
	XLogRecPtr	pd_lsn;
};

/* Format of the header of the double-write file */
struct DoubleBufHeader
{
	uint32		cksum;
	int32		writeLen;
	uint32		pad1;
	uint32		pad2;
	struct DoubleBufItem items[0];
};

extern char *dwBlocks;
extern struct DWBufferInfo *dwInfo;
extern int batched_buffer_writes;
extern bool doubleWrites;
extern bool page_checksum;


extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileNode rnode, BackendId backend);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum);
extern void smgrsetowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclearowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrcloseall(void);
extern void smgrclosenode(RelFileNodeBackend rnode);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrdounlink(SMgrRelation reln, bool isRedo);
extern void smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo);
extern void smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrprefetch(SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum);
extern void smgrread(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, char *buffer, bool *cksumMismatch);
extern void smgrwrite(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, char *buffer, bool skipFsync,
			bool dw);
extern void smgrbwrite(int writeLen, struct SMgrWriteList *writeList,
		   File doubleWriteFile);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncate(SMgrRelation reln, ForkNumber forknum,
			 BlockNumber nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void smgrpreckpt(void);
extern void smgrsync(void);
extern void smgrpostckpt(void);
extern void AtEOXact_SMgr(void);
extern void FlushDoubleWriteBuffer(int dwIndex);
extern char *DoubleWriteFileName(int index);
extern void RecoverDoubleWriteFile(void);


/* internals: move me elsewhere -- ay 7/94 */

/* in md.c */
extern void mdinit(void);
extern void mdclose(SMgrRelation reln, ForkNumber forknum);
extern void mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool mdexists(SMgrRelation reln, ForkNumber forknum);
extern void mdunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void mdextend(SMgrRelation reln, ForkNumber forknum,
		 BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdprefetch(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber blocknum);
extern void mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer);
extern void mdwrite(SMgrRelation reln, ForkNumber forknum,
		BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdbwrite(int writeLen, struct SMgrWriteList *writeList,
		 File doubleWriteFile, char *doublebuf);
extern void mdwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber mdnblocks(SMgrRelation reln, ForkNumber forknum);
extern void mdtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber nblocks);
extern void mdimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void mdpreckpt(void);
extern void mdsync(void);
extern void mdpostckpt(void);

extern void SetForwardFsyncRequests(void);
extern void RememberFsyncRequest(RelFileNode rnode, ForkNumber forknum,
					 BlockNumber segno);
extern void ForgetRelationFsyncRequests(RelFileNode rnode, ForkNumber forknum);
extern void ForgetDatabaseFsyncRequests(Oid dbid);

#endif							/* SMGR_H */
