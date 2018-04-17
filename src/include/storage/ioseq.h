/*
 *-------------------------------------------------------------------------
 *
 * ioseq.h
 *	  Exports from storage/buffer/ioseq.c.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#ifndef _IOSEQ_H
#define _IOSEQ_H

#include "storage/buf_internals.h"

/* Structure used for sorting buffers on tag and eviction priority */
typedef struct {
	int idx;       /* Index in BufferDescriptors */
	int clock;     /* Sequence number in clock order: clock(nextVictim) = 0 */
	BufferTag tag; /* Tag of the buffer */
} BufferIdxTag;

extern bool sortDirtyBuffers;
extern BufferIdxTag *ioSeqList;

void IOSeq_Init(void);
Size IOSeq_ShmemSize(void);
void IOSeq_Sort(BufferIdxTag *entries, unsigned numEntries);
unsigned IOSeq_ClockStart(BufferIdxTag *entries, unsigned numEntries);

#endif   /* _IOSEQ_H */
