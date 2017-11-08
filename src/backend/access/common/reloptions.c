/*-------------------------------------------------------------------------
 *
 * reloptions.c
 *	  Support for options relotions (pg_class.reloptions).
 *	  Reloptions for non-Access Metod relations are defined here.
 *	  There is also extractRelOptions function to extract reloptions
 *	  from pg_class table for all kind of relations.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/reloptions.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <float.h>

#include "access/htup_details.h"
#include "access/options.h"
#include "access/reloptions.h"
#include "commands/tablespace.h"
#include "utils/attoptcache.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"

void add_autovacuum_options(options_catalog * catalog, int base_offset,
					   bool for_toast);

/*
 * Extract and parse reloptions from a pg_class tuple.
 *
 * This is a low-level routine, expected to be used by relcache code and
 * callers that do not have a table's relcache entry (e.g. autovacuum).  For
 * other uses, consider grabbing the rd_options pointer from the relcache entry
 * instead.
 *
 * tupdesc is pg_class' tuple descriptor.  amoptions is a pointer to the index
 * AM's options parser function in the case of a tuple corresponding to an
 * index, or NULL otherwise.
 */
bytea *
extractRelOptions(HeapTuple tuple, TupleDesc tupdesc,
				  amrelopt_catalog_function catalog_fun)
{
	bytea	   *options = NULL;
	bool		isnull;
	Datum		datum;
	options_catalog *catalog = NULL;
	Form_pg_class classForm;

	datum = fastgetattr(tuple,
						Anum_pg_class_reloptions,
						tupdesc,
						&isnull);
	if (isnull)
		return NULL;

	classForm = (Form_pg_class) GETSTRUCT(tuple);

	/* Parse into appropriate format; don't error out here */
	switch (classForm->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			catalog = get_heap_relopt_catalog();
			break;
		case RELKIND_TOASTVALUE:
			catalog = get_toast_relopt_catalog();
			break;
		case RELKIND_VIEW:
			catalog = get_view_relopt_catalog();
			break;
		case RELKIND_INDEX:
			if (catalog_fun)
				catalog = catalog_fun();
			break;
		case RELKIND_FOREIGN_TABLE:
			options = NULL;
			break;
		case RELKIND_PARTITIONED_TABLE:
			catalog = NULL; /* No options for parted table for now */
			break;
		default:
			Assert(false);		/* can't get here */
			options = NULL;		/* keep compiler quiet */
			break;
	}

	if (catalog)
		options = optionsTextArrayToBytea(catalog, datum);
	return options;
}


/*
 * Here goes functions that build catalogs for non-index relations. Catalogs
 * for index relations are available from Access Methods, all other
 * relation catalogs are here
 */

/*
 * Both heap and toast relation have almost the same autovacuum set of options.
 * These autovacuum options are stored in the same structure (AutoVacOpts)
 * that is a part of both HeapOptions and ToastOptions.
 *
 * Function add_autovacuum_options adds to the "catalog" catalog autovacuum
 * options definition. In binary mode this options are saved into AutoVacOpts
 * structure, which is located in result bytea chunk with base_offset offset.
 *
 * Heap has two options (autovacuum_analyze_threshold and
 * autovacuum_analyze_scale_factor) that is not used for toast. So for toast
 * case this options are added to catalog with OPTION_DEFINITION_FLAG_REJECT
 * flag set, so postgres will reject this toast options from the CREATE or
 * ALTER command, but still will save default value in binary representation
 */

void
add_autovacuum_options(options_catalog * catalog, int base_offset,
					   bool for_toast)
{
	optionsCatalogAddItemBool(catalog, "autovacuum_enabled",
							  "Enables autovacuum in this relation",
							  ShareUpdateExclusiveLock,
					  0, base_offset + offsetof(AutoVacOpts, enabled), true);

	optionsCatalogAddItemInt(catalog, "autovacuum_vacuum_threshold",
				"Minimum number of tuple updates or deletes prior to vacuum",
							 ShareUpdateExclusiveLock,
					0, base_offset + offsetof(AutoVacOpts, vacuum_threshold),
							 -1, 0, INT_MAX);

	optionsCatalogAddItemInt(catalog, "autovacuum_analyze_threshold",
				"Minimum number of tuple updates or deletes prior to vacuum",
							 ShareUpdateExclusiveLock,
							 for_toast ? OPTION_DEFINITION_FLAG_REJECT : 0,
					  base_offset + offsetof(AutoVacOpts, analyze_threshold),
							 -1, 0, INT_MAX);

	optionsCatalogAddItemInt(catalog, "autovacuum_vacuum_cost_delay",
						 "Vacuum cost delay in milliseconds, for autovacuum",
							 ShareUpdateExclusiveLock,
				   0, base_offset + offsetof(AutoVacOpts, vacuum_cost_delay),
							 -1, 0, 100);

	optionsCatalogAddItemInt(catalog, "autovacuum_vacuum_cost_limit",
			   "Vacuum cost amount available before napping, for autovacuum",
							 ShareUpdateExclusiveLock,
				   0, base_offset + offsetof(AutoVacOpts, vacuum_cost_limit),
							 -1, 0, 10000);

	optionsCatalogAddItemInt(catalog, "autovacuum_freeze_min_age",
	 "Minimum age at which VACUUM should freeze a table row, for autovacuum",
							 ShareUpdateExclusiveLock,
					  0, base_offset + offsetof(AutoVacOpts, freeze_min_age),
							 -1, 0, 1000000000);

	optionsCatalogAddItemInt(catalog, "autovacuum_freeze_max_age",
	"Age at which to autovacuum a table to prevent transaction ID wraparound",
							 ShareUpdateExclusiveLock,
					  0, base_offset + offsetof(AutoVacOpts, freeze_max_age),
							 -1, 100000, 2000000000);

	optionsCatalogAddItemInt(catalog, "autovacuum_freeze_table_age",
							 "Age at which VACUUM should perform a full table sweep to freeze row versions",
							 ShareUpdateExclusiveLock,
					0, base_offset + offsetof(AutoVacOpts, freeze_table_age),
							 -1, 0, 2000000000);

	optionsCatalogAddItemInt(catalog, "autovacuum_multixact_freeze_min_age",
							 "Minimum multixact age at which VACUUM should freeze a row multixact's, for autovacuum",
							 ShareUpdateExclusiveLock,
			0, base_offset + offsetof(AutoVacOpts, multixact_freeze_min_age),
							 -1, 0, 1000000000);

	optionsCatalogAddItemInt(catalog, "autovacuum_multixact_freeze_max_age",
							 "Multixact age at which to autovacuum a table to prevent multixact wraparound",
							 ShareUpdateExclusiveLock,
			0, base_offset + offsetof(AutoVacOpts, multixact_freeze_max_age),
							 -1, 10000, 2000000000);

	optionsCatalogAddItemInt(catalog, "autovacuum_multixact_freeze_table_age",
							 "Age of multixact at which VACUUM should perform a full table sweep to freeze row versions",
							 ShareUpdateExclusiveLock,
		  0, base_offset + offsetof(AutoVacOpts, multixact_freeze_table_age),
							 -1, 0, 2000000000);

	optionsCatalogAddItemInt(catalog, "log_autovacuum_min_duration",
							 "Sets the minimum execution time above which autovacuum actions will be logged",
							 ShareUpdateExclusiveLock,
					0, base_offset + offsetof(AutoVacOpts, log_min_duration),
							 -1, -1, INT_MAX);

	optionsCatalogAddItemReal(catalog, "autovacuum_vacuum_scale_factor",
							  "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples",
							  ShareUpdateExclusiveLock,
				 0, base_offset + offsetof(AutoVacOpts, vacuum_scale_factor),
							  -1, 0.0, 100.0);

	optionsCatalogAddItemReal(catalog, "autovacuum_analyze_scale_factor",
							  "Number of tuple inserts, updates or deletes prior to analyze as a fraction of reltuples",
							  ShareUpdateExclusiveLock,
							  for_toast ? OPTION_DEFINITION_FLAG_REJECT : 0,
				   base_offset + offsetof(AutoVacOpts, analyze_scale_factor),
							  -1, 0.0, 100.0);
}

/*
 * get_heap_relopt_catalog
 *		Returns an options catalog for heap relation.
 */
static options_catalog *heap_relopt_catalog = NULL;

options_catalog *
get_heap_relopt_catalog(void)
{
	if (!heap_relopt_catalog)
	{
		heap_relopt_catalog = allocateOptionsCatalog(NULL, sizeof(HeapOptions),
													 14 + 4);	/* 14 - for autovacuum
																 * options, 4 - core
																 * heap options */

		optionsCatalogAddItemInt(heap_relopt_catalog, "fillfactor",
								 "Packs table pages only to this percentag",
								 ShareUpdateExclusiveLock,		/* since it applies only
																 * to later inserts */
								 0, offsetof(HeapOptions, fillfactor),
						  HEAP_DEFAULT_FILLFACTOR, HEAP_MIN_FILLFACTOR, 100);

		add_autovacuum_options(heap_relopt_catalog,
							   offsetof(HeapOptions, autovacuum), false);

		optionsCatalogAddItemBool(heap_relopt_catalog, "user_catalog_table",
								  "Declare a table as an additional catalog table, e.g. for the purpose of logical replication",
								  AccessExclusiveLock,
								0, offsetof(HeapOptions, user_catalog_table),
								  false);

		optionsCatalogAddItemInt(heap_relopt_catalog, "parallel_workers",
								 "Number of parallel processes that can be used per executor node for this relation",
								 ShareUpdateExclusiveLock,
								 0, offsetof(HeapOptions, parallel_workers),
								 -1, 0, 1024);

		/*
		 * A WITH OID / WITHOUT OIDS expressions are converted by syntax
		 * parser into "oids" relation option, but this option is processed by
		 * interpretOidsOption(), that does not use core options code. That is
		 * why here we should just ignore this option as if it does not exist.
		 * Do it with OPTION_DEFINITION_FLAG_IGNORE flag
		 */
		optionsCatalogAddItemBool(heap_relopt_catalog, "oids",
				 "Option used for WITH OIDS expression. Processed elsewhere",
								  NoLock,
								  OPTION_DEFINITION_FLAG_IGNORE, -1,
								  false);
	}
	return heap_relopt_catalog;
}

/*
 * get_toast_relopt_catalog
 *		Returns an options catalog for toast relation.
 */

static options_catalog *toast_relopt_catalog = NULL;

options_catalog *
get_toast_relopt_catalog(void)
{
	if (!toast_relopt_catalog)
	{
		toast_relopt_catalog = allocateOptionsCatalog("toast", sizeof(ToastOptions),
													  14);		/* 14 - for autovacuum
																 * options */
		add_autovacuum_options(toast_relopt_catalog,
							   offsetof(ToastOptions, autovacuum), true);
	}
	return toast_relopt_catalog;
}

/*
 * get_view_relopt_catalog
 *		Returns an options catalog for view relation.
 */
static options_catalog *view_relopt_catalog = NULL;

options_catalog *
get_view_relopt_catalog(void)
{
	static const char *enum_names[] = VIEW_OPTION_CHECK_OPTION_VALUE_NAMES;

	if (!view_relopt_catalog)
	{
		view_relopt_catalog = allocateOptionsCatalog(NULL,
													 sizeof(ViewOptions), 2);

		optionsCatalogAddItemBool(view_relopt_catalog, "security_barrier",
								  "View acts as a row security barrier",
								  AccessExclusiveLock,
						  0, offsetof(ViewOptions, security_barrier), false);

		optionsCatalogAddItemEnum(view_relopt_catalog, "check_option",
					"View has WITH CHECK OPTION defined (local or cascaded)",
								  AccessExclusiveLock, 0,
								  offsetof(ViewOptions, check_option),
								  enum_names,
								  VIEW_OPTION_CHECK_OPTION_NOT_SET);
	}
	return view_relopt_catalog;
}

/*
 * get_attribute_options_catalog
 *		Returns an options catalog for heap attributes
 */
static options_catalog *attribute_options_catalog = NULL;

options_catalog *
get_attribute_options_catalog(void)
{
	if (!attribute_options_catalog)
	{
		attribute_options_catalog = allocateOptionsCatalog(NULL,
												   sizeof(AttributeOpts), 2);

		optionsCatalogAddItemReal(attribute_options_catalog, "n_distinct",
								  "Sets the planner's estimate of the number of distinct values appearing in a column (excluding child relations)",
								  ShareUpdateExclusiveLock,
				   0, offsetof(AttributeOpts, n_distinct), 0, -1.0, DBL_MAX);

		optionsCatalogAddItemReal(attribute_options_catalog,
								  "n_distinct_inherited",
								  "Sets the planner's estimate of the number of distinct values appearing in a column (including child relations",
								  ShareUpdateExclusiveLock,
		 0, offsetof(AttributeOpts, n_distinct_inherited), 0, -1.0, DBL_MAX);
	}
	return attribute_options_catalog;
}

/*
 * get_tablespace_options_catalog
 *		Returns an options catalog for tablespaces
 */
static options_catalog *tablespace_options_catalog = NULL;

options_catalog *
get_tablespace_options_catalog(void)
{
	if (!tablespace_options_catalog)
	{
		tablespace_options_catalog = allocateOptionsCatalog(NULL,
												  sizeof(TableSpaceOpts), 3);

		optionsCatalogAddItemReal(tablespace_options_catalog,
								  "random_page_cost",
								  "Sets the planner's estimate of the cost of a nonsequentially fetched disk page",
								  ShareUpdateExclusiveLock,
			0, offsetof(TableSpaceOpts, random_page_cost), -1, 0.0, DBL_MAX);

		optionsCatalogAddItemReal(tablespace_options_catalog, "seq_page_cost",
								  "Sets the planner's estimate of the cost of a sequentially fetched disk page",
								  ShareUpdateExclusiveLock,
			   0, offsetof(TableSpaceOpts, seq_page_cost), -1, 0.0, DBL_MAX);

		optionsCatalogAddItemInt(tablespace_options_catalog,
								 "effective_io_concurrency",
								 "Number of simultaneous requests that can be handled efficiently by the disk subsystem",
								 ShareUpdateExclusiveLock,
					   0, offsetof(TableSpaceOpts, effective_io_concurrency),
#ifdef USE_PREFETCH
								 -1, 0, MAX_IO_CONCURRENCY
#else
								 0, 0, 0
#endif
			);
	}
	return tablespace_options_catalog;
}

/*
 * Determine the required LOCKMODE from an option list.
 *
 * Called from AlterTableGetLockLevel(), see that function
 * for a longer explanation of how this works.
 */
LOCKMODE
AlterTableGetRelOptionsLockLevel(Relation rel, List *defList)
{
	LOCKMODE	lockmode = NoLock;
	ListCell   *cell;
	options_catalog *catalog = NULL;

	if (defList == NIL)
		return AccessExclusiveLock;

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_TOASTVALUE:
			catalog = get_toast_relopt_catalog();
			break;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			catalog = get_heap_relopt_catalog();
			break;
		case RELKIND_INDEX:
			catalog = rel->rd_amroutine->amrelopt_catalog();
			break;
		case RELKIND_VIEW:
			catalog = get_view_relopt_catalog();
			break;
		case RELKIND_PARTITIONED_TABLE:
			catalog = NULL; /* No options for parted table for now */
			break;
		default:
			Assert(false);		/* can't get here */
			break;
	}

	Assert(catalog);			/* No catalog - no reloption change. Should
								 * not get here */

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		int			i;

		for (i = 0; i < catalog->num; i++)
		{
			option_definition_basic *gen = catalog->definitions[i];

			if (pg_strcasecmp(gen->name,
							  def->defname) == 0)
				if (lockmode < gen->lockmode)
					lockmode = gen->lockmode;
		}
	}
	return lockmode;
}
