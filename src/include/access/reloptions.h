/*-------------------------------------------------------------------------
 *
 * reloptions.h
 *	  Support for relation and tablespace options (pg_class.reloptions
 *	  and pg_tablespace.spcoptions)
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/reloptions.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELOPTIONS_H
#define RELOPTIONS_H

#include "access/amapi.h"
#include "access/options.h"


/* reloption namespaces allowed for heaps -- currently only TOAST */
#define HEAP_RELOPT_NAMESPACES { "toast", NULL }


extern bytea *extractRelOptions(HeapTuple tuple, TupleDesc tupdesc,
				  amrelopt_catalog_function catalog_fun);

/*
 * Functions that creates option catalog instances for heap, toast, view
 * and other object types that are part of the postgres core
 */

extern options_catalog *get_heap_relopt_catalog(void);
extern options_catalog *get_toast_relopt_catalog(void);

extern options_catalog *get_view_relopt_catalog(void);
extern options_catalog *get_attribute_options_catalog(void);
extern options_catalog *get_tablespace_options_catalog(void);
extern LOCKMODE AlterTableGetRelOptionsLockLevel(Relation rel, List *defList);

#endif							/* RELOPTIONS_H */
