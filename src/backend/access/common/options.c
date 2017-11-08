/*-------------------------------------------------------------------------
 *
 * options.c
 *	  Core support for options used for relotions (pg_class.reloptions),
 *	  attoptions (pg_attribute.attoptions), and can be used for other
 *	  kinds of options.
 *
 *	  Here you can find functions that allow to define available options
 *	  for certain object (certain retaion type, attribute type etc), and
 *	  functions that allows to convert options from one represenation to
 *	  another and validate them.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/options.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/options.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"

/*
 * Any object (i.e. relation type) that want to use options, should create an
 * options catalog using  allocateOptionsCatalog function, fill it with
 * optionsCatalogAddItemXxxx functions with definition of available options.
 * Then instance of this object can take option values from syntax analyzer
 * when instance in created using SQL command or from attribute of
 * pg_catalog.yyyy table, when object that already exists is loaded form
 * pg_catalog.
 *
 * Now options are mainly used for all kind or relations (pg_class.reloption)
 * and also for attributes (pg_attribute.attoptions) and tablespaces
 * (pg_tablespace.spcoptions)
 *
 * Options catalog for index relation options should be available from
 * amrelopt_catalog access method function. For non-index relations and for
 * other objects catalogs are available via functions from this file. E.g.
 * get_heap_relopt_catalog for heap relation options catalog.
 *
 * This file has two sections:
 *
 * 1. Functions for managing option catalogs
 *
 * 2. Functions for manipulating and transforming options
 *
 * More explanations are available at the beginning of each section
 */


static option_definition_basic *allocateOptionDefinition(int type, char *name,
						 char *desc, LOCKMODE lockmode,
						 option_definition_flags flags, int struct_offset);

static void parse_one_option(option_value * option, char *text_str,
				 int text_len, bool validate);
static void *optionsAllocateBytea(options_catalog * catalog, List *options);


static List *
optionsDefListToRawValues(List *defList, options_parse_mode
						  parse_mode);
static Datum optionsValuesToTextArray(List *options_values);
static List *optionsTextArrayToRawValues(Datum array_datum);
static List *optionsMergeOptionValues(List *old_options, List *new_options);
static List *optionsParseRawValues(List *raw_values, options_catalog * catalog,
					  options_parse_mode mode);
static bytea *optionsValuesToBytea(List *options, options_catalog * catalog);


/*
 * Options catalog functions
 */

/*
 * Options catalog describes options available for certain object. Catalog has
 * all necessary information for parsing transforming and validating options
 * for an object. All parsing/validation/transformation functions should not
 * know any details of option implementation for certain object, all this
 * information should be stored in catalog instead and interpreted by
 * pars/valid/transf functions blindly.
 *
 * The heart of the option catalog is an array of option definitions.  Options
 * definition specifies name of option, type, range of acceptable values, and
 * default value.
 *
 * Options values can be one of the following types: bool, int, real, enum,
 * string. For more info see "option_type" and "optionsCatalogAddItemYyyy"
 * functions.
 *
 * Option definition flags allows to define parser behavior for special (or not
 * so special) cases. See option_definition_flags for more info.
 *
 * Options and Lock levels:
 *
 * The default choice for any new option should be AccessExclusiveLock.
 * In some cases the lock level can be reduced from there, but the lock
 * level chosen should always conflict with itself to ensure that multiple
 * changes aren't lost when we attempt concurrent changes.
 * The choice of lock level depends completely upon how that parameter
 * is used within the server, not upon how and when you'd like to change it.
 * Safety first. Existing choices are documented here, and elsewhere in
 * backend code where the parameters are used.
 *
 * In general, anything that affects the results obtained from a SELECT must be
 * protected by AccessExclusiveLock.
 *
 * Autovacuum related parameters can be set at ShareUpdateExclusiveLock
 * since they are only used by the AV procs and don't change anything
 * currently executing.
 *
 * Fillfactor can be set because it applies only to subsequent changes made to
 * data blocks, as documented in heapio.c
 *
 * n_distinct options can be set at ShareUpdateExclusiveLock because they
 * are only used during ANALYZE, which uses a ShareUpdateExclusiveLock,
 * so the ANALYZE will not be affected by in-flight changes. Changing those
 * values has no affect until the next ANALYZE, so no need for stronger lock.
 *
 * Planner-related parameters can be set with ShareUpdateExclusiveLock because
 * they only affect planning and not the correctness of the execution. Plans
 * cannot be changed in mid-flight, so changes here could not easily result in
 * new improved plans in any case. So we allow existing queries to continue
 * and existing plans to survive, a small price to pay for allowing better
 * plans to be introduced concurrently without interfering with users.
 *
 * Setting parallel_workers is safe, since it acts the same as
 * max_parallel_workers_per_gather which is a USERSET parameter that doesn't
 * affect existing plans or queries.
*/

/*
 * allocateOptionsCatalog
 *		Allocate memory for a new catalog and initializes structure members.
 *
 * namespace - name of a namespace in which all items of the catalog
 * exists (E.g. namespace.option=value). For now postgres uses only toast.
 * namespace for tables.
 *
 * size_of_bytea - size of bytea-packed C-structure where all option values
 * would be finally mapped, so it can be fast and easily used by pg core
 *
 * num_items_expected - expected number of items in options catalog. If it is
 * set to positive value, proper chunk of memory would be allocated, and postgres
 * will assert if you try to add more options to the catalog. If it is set to -1, then
 * you can add any number of options to catalog, memory for entries would be dynamically
 * reallocated while adding. This feature was left for backward compatibility and is
 * not encouraged for production usage.
 */

options_catalog *
allocateOptionsCatalog(char *namespace, int size_of_bytea, int num_items_expected)
{
	MemoryContext oldcxt;
	options_catalog *catalog;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	catalog = palloc(sizeof(options_catalog));
	if (namespace)
	{
		catalog->namespace = palloc(strlen(namespace) + 1);
		strcpy(catalog->namespace, namespace);
	}
	else
		catalog->namespace = NULL;
	if (num_items_expected > 0)
	{
		catalog->num_allocated = num_items_expected;
		catalog->forbid_realloc = true;
		catalog->definitions = palloc(
				 catalog->num_allocated * sizeof(option_definition_basic *));
	}
	else
	{
		catalog->num_allocated = 0;
		catalog->forbid_realloc = false;
		catalog->definitions = NULL;
	}
	catalog->num = 0;
	catalog->struct_size = size_of_bytea;
	catalog->postprocess_fun = NULL;
	MemoryContextSwitchTo(oldcxt);
	return catalog;
}

/*
 * optionCatalogAddItem
 *		Add an already-created option definition to the catalog
 */
static void
optionCatalogAddItem(option_definition_basic * newoption,
					 options_catalog * catalog)
{
	if (catalog->num >= catalog->num_allocated)
	{
		MemoryContext oldcxt;

		Assert(!catalog->forbid_realloc);
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);

		if (catalog->num_allocated == 0)
		{
			catalog->num_allocated = 8;
			catalog->definitions = palloc(
				 catalog->num_allocated * sizeof(option_definition_basic *));
		}
		else
		{
			catalog->num_allocated *= 2;
			catalog->definitions = repalloc(catalog->definitions,
				 catalog->num_allocated * sizeof(option_definition_basic *));
		}
		MemoryContextSwitchTo(oldcxt);
	}
	catalog->definitions[catalog->num] = newoption;
	catalog->num++;
}

/*
 * allocateOptionDefinition
 *		Allocate a new option definition and initialize the type-agnostic
 *		fields
 */
static option_definition_basic *
allocateOptionDefinition(int type, char *name, char *desc, LOCKMODE lockmode,
						 option_definition_flags flags, int struct_offset)
{
	MemoryContext oldcxt;
	size_t		size;
	option_definition_basic *newoption;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	switch (type)
	{
		case OPTION_TYPE_BOOL:
			size = sizeof(option_definition_bool);
			break;
		case OPTION_TYPE_INT:
			size = sizeof(option_definition_int);
			break;
		case OPTION_TYPE_REAL:
			size = sizeof(option_definition_real);
			break;
		case OPTION_TYPE_ENUM:
			size = sizeof(option_definition_enum);
			break;
		case OPTION_TYPE_STRING:
			size = sizeof(option_definition_string);
			break;
		default:
			elog(ERROR, "unsupported reloption type %d", type);
			return NULL;		/* keep compiler quiet */
	}

	newoption = palloc(size);

	newoption->name = pstrdup(name);
	if (desc)
		newoption->desc = pstrdup(desc);
	else
		newoption->desc = NULL;
	newoption->type = type;
	newoption->lockmode = lockmode;
	newoption->flags = flags;
	newoption->struct_offset = struct_offset;

	MemoryContextSwitchTo(oldcxt);

	return newoption;
}

/*
 * optionsCatalogAddItemBool
 *		Add a new boolean option definition to the catalog
 */
void
optionsCatalogAddItemBool(options_catalog * catalog, char *name, char *desc,
						  LOCKMODE lockmode, option_definition_flags flags,
						  int struct_offset, bool default_val)
{
	option_definition_bool *catalog_item;

	catalog_item = (option_definition_bool *)
		allocateOptionDefinition(OPTION_TYPE_BOOL, name, desc, lockmode,
								 flags, struct_offset);

	catalog_item->default_val = default_val;

	optionCatalogAddItem((option_definition_basic *) catalog_item, catalog);
}

/*
 * optionsCatalogAddItemInt
 *		Add a new integer option definition to the catalog
 */
void
optionsCatalogAddItemInt(options_catalog * catalog, char *name,
				char *desc, LOCKMODE lockmode, option_definition_flags flags,
				int struct_offset, int default_val, int min_val, int max_val)
{
	option_definition_int *catalog_item;

	catalog_item = (option_definition_int *)
		allocateOptionDefinition(OPTION_TYPE_INT, name, desc, lockmode,
								 flags, struct_offset);

	catalog_item->default_val = default_val;
	catalog_item->min = min_val;
	catalog_item->max = max_val;

	optionCatalogAddItem((option_definition_basic *) catalog_item, catalog);
}

/*
 * optionsCatalogAddItemReal
 *		Add a new float option to the catalog
 */
void
optionsCatalogAddItemReal(options_catalog * catalog, char *name, char *desc,
		 LOCKMODE lockmode, option_definition_flags flags, int struct_offset,
						  double default_val, double min_val, double max_val)
{
	option_definition_real *catalog_item;

	catalog_item = (option_definition_real *)
		allocateOptionDefinition(OPTION_TYPE_REAL, name, desc, lockmode,
								 flags, struct_offset);

	catalog_item->default_val = default_val;
	catalog_item->min = min_val;
	catalog_item->max = max_val;

	optionCatalogAddItem((option_definition_basic *) catalog_item, catalog);
}

/*
 * optionsCatalogAddItemEnum
 *		Add a new enum option to the catalog
 *
 * "allowed_values" is a pointer to a NULL-terminated char* array (last item of
 * an array should be null). This array contains a list of acceptable values
 * for the option.
 *
 * "default_val" is a number of item in allowed_values array that should be
 * set as default value. If you want to handle "option was not set" special
 * case, you can set default_val to -1
 */
void
optionsCatalogAddItemEnum(options_catalog * catalog, char *name, char *desc,
		 LOCKMODE lockmode, option_definition_flags flags, int struct_offset,
						  const char **allowed_values, int default_val)
{
	option_definition_enum *catalog_item;

	catalog_item = (option_definition_enum *)
		allocateOptionDefinition(OPTION_TYPE_ENUM, name, desc, lockmode,
								 flags, struct_offset);

	catalog_item->default_val = default_val;
	catalog_item->allowed_values = allowed_values;

	optionCatalogAddItem((option_definition_basic *) catalog_item, catalog);
}

/*
 * optionsCatalogAddItemString
 *		Add a new string option definition to the catalog
 *
 * "validator" is an optional function pointer that can be used to test the
 * validity of the values.  It must elog(ERROR) when the argument string is
 * not acceptable for the variable.  Note that the default value must pass
 * the validation.
 */
void
optionsCatalogAddItemString(options_catalog * catalog, char *name, char *desc,
		 LOCKMODE lockmode, option_definition_flags flags, int struct_offset,
						 char *default_val, validate_string_relopt validator)
{
	option_definition_string *catalog_item;

	/* make sure the validator/default combination is sane */
	if (validator)
		(validator) (default_val);

	catalog_item = (option_definition_string *)
		allocateOptionDefinition(OPTION_TYPE_STRING, name, desc, lockmode,
								 flags, struct_offset);
	catalog_item->validate_cb = validator;

	if (default_val)
		catalog_item->default_val = MemoryContextStrdup(TopMemoryContext,
														default_val);
	else
		catalog_item->default_val = NULL;
	optionCatalogAddItem((option_definition_basic *) catalog_item, catalog);
}


/*
 * Options transform functions
 */

/*
 * Option values exists in five representations: DefList, TextArray, Values and
 * Bytea:
 *
 * DefList: Is a List of DefElem structures, that comes from syntax analyzer.
 * It can be transformed to Values representation for further parsing and
 * validating
 *
 * Values: A List of option_value structures. Is divided into two subclasses:
 * RawValues, when values are already transformed from DefList or TextArray,
 * but not parsed yet. (In this case you should use raw_name and raw_value
 * structure members to see option content). ParsedValues (or just simple
 * Values) is crated after finding a definition for this option in a catalog
 * and after parsing of the raw value. For ParsedValues content is stored in
 * values structure member, and name can be taken from option definition in gen
 * structure member.  Actually Value list can have both Raw and Parsed values,
 * as we do not validate options that came from database, and db option that
 * does not exist in catalog is just ignored, and kept as RawValues
 *
 * TextArray: The representation in which  options for existing object comes
 * and goes from/to database; for example from pg_class.reloptions. It is a
 * plain TEXT[] db object with name=value text inside. This representation can
 * be transformed into Values for further processing, using options catalog.
 *
 * Bytea: Is a binary representation of options. Each object that has code that
 * uses options, should create a C-structure for this options, with varlen
 * 4-byte header in front of the data; all items of options catalog should have
 * an offset of a corresponding binary data in this structure, so transform
 * function can put this data in the correct place. One can transform options
 * data from values representation into Bytea, using catalog data, and then use
 * it as a usual Datum object, when needed. This Datum should be cached
 * somewhere (for example in rel->rd_options for relations) when object that
 * has option is loaded from db.
 */


/* optionsDefListToRawValues
 *		Converts option values that came from syntax analyzer (DefList) into
 *		Values List.
 *
 * No parsing is done here except for checking that RESET syntax is correct
 * (syntax analyzer do not see difference between SET and RESET cases, we
 * should treat it here manually
 */
static List *
optionsDefListToRawValues(List *defList, options_parse_mode parse_mode)
{
	ListCell   *cell;
	List	   *result = NIL;

	foreach(cell, defList)
	{
		option_value *option_dst;
		DefElem    *def = (DefElem *) lfirst(cell);
		char	   *value;

		option_dst = palloc(sizeof(option_value));

		if (def->defnamespace)
		{
			option_dst->namespace = palloc(strlen(def->defnamespace) + 1);
			strcpy(option_dst->namespace, def->defnamespace);
		}
		else
		{
			option_dst->namespace = NULL;
		}
		option_dst->raw_name = palloc(strlen(def->defname) + 1);
		strcpy(option_dst->raw_name, def->defname);

		if (parse_mode & OPTIONS_PARSE_MODE_FOR_RESET)
		{
			/*
			 * If this option came from RESET statement we should throw error
			 * it it brings us name=value data, as syntax analyzer do not
			 * prevent it
			 */
			if (def->arg != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("RESET must not include values for parameters")));

			option_dst->status = OPTION_VALUE_STATUS_FOR_RESET;
		}
		else
		{
			/*
			 * For SET statement we should treat (name) expression as if it is
			 * actually (name=true) so do it here manually. In other cases
			 * just use value as we should use it
			 */
			option_dst->status = OPTION_VALUE_STATUS_RAW;
			if (def->arg != NULL)
				value = defGetString(def);
			else
				value = "true";
			option_dst->raw_value = palloc(strlen(value) + 1);
			strcpy(option_dst->raw_value, value);
		}

		result = lappend(result, option_dst);
	}
	return result;
}

/*
 * optionsValuesToTextArray
 *		Converts List of option_values into TextArray
 *
 *	Convertation is made to put options into database (e.g. in
 *	pg_class.reloptions for all relation options)
 */

Datum
optionsValuesToTextArray(List *options_values)
{
	ArrayBuildState *astate = NULL;
	ListCell   *cell;
	Datum		result;

	foreach(cell, options_values)
	{
		option_value *option = (option_value *) lfirst(cell);
		const char *name;
		char	   *value;
		text	   *t;
		int			len;

		/*
		 * Raw value were not cleared while parsing, so instead of converting
		 * it back, just use it to store value as text
		 */
		value = option->raw_value;

		Assert(option->status != OPTION_VALUE_STATUS_EMPTY);

		/*
		 * Name will be taken from option definition, if option were parsed or
		 * from raw_name if option were not parsed for some reason
		 */
		if (option->status == OPTION_VALUE_STATUS_PARSED)
			name = option->gen->name;
		else
			name = option->raw_name;

		/*
		 * Now build "name=value" string and append it to the array
		 */
		len = VARHDRSZ + strlen(name) + strlen(value) + 1;
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "%s=%s", name, value);
		astate = accumArrayResult(astate, PointerGetDatum(t), false,
								  TEXTOID, CurrentMemoryContext);
	}
	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;

	return result;
}

/*
 * optionsTextArrayToRawValues
 *		Converts options from TextArray format into RawValues list.
 *
 *	This function is used to convert options data that comes from database to
 *	List of option_values, for further parsing, and, in the case of ALTER
 *	command, for merging with new option values.
 */
static List *
optionsTextArrayToRawValues(Datum array_datum)
{
	List	   *result = NIL;

	if (PointerIsValid(DatumGetPointer(array_datum)))
	{
		ArrayType  *array = DatumGetArrayTypeP(array_datum);
		Datum	   *options;
		int			noptions;
		int			i;

		deconstruct_array(array, TEXTOID, -1, false, 'i',
						  &options, NULL, &noptions);

		for (i = 0; i < noptions; i++)
		{
			option_value *option_dst;
			char	   *text_str = VARDATA(options[i]);
			int			text_len = VARSIZE(options[i]) - VARHDRSZ;
			int			i;
			int			name_len = -1;
			char	   *name;
			int			raw_value_len;
			char	   *raw_value;

			/*
			 * Find position of '=' sign and treat id as a separator between
			 * name and value in "name=value" item
			 */
			for (i = 0; i < text_len; i = i + pg_mblen(text_str))
			{
				if (text_str[i] == '=')
				{
					name_len = i;
					break;
				}
			}
			Assert(name_len >= 1);		/* Just in case */

			raw_value_len = text_len - name_len - 1;

			/*
			 * Copy name from src
			 */
			name = palloc(name_len + 1);
			memcpy(name, text_str, name_len);
			name[name_len] = '\0';

			/*
			 * Copy value from src
			 */
			raw_value = palloc(raw_value_len + 1);
			memcpy(raw_value, text_str + name_len + 1, raw_value_len);
			raw_value[raw_value_len] = '\0';

			/*
			 * Create new option_value item
			 */
			option_dst = palloc(sizeof(option_value));
			option_dst->status = OPTION_VALUE_STATUS_RAW;
			option_dst->raw_name = name;
			option_dst->raw_value = raw_value;

			result = lappend(result, option_dst);
		}
	}
	return result;
}

/*
 * optionsMergeOptionValues
 *		Merges two lists of option_values into one list
 *
 * This function is used to merge two Values list into one. It is used for all
 * kinds of ALTER commands when existing options are merged|replaced with new
 * options list. This function also process RESET variant of ALTER command. It
 * merges two lists as usual, and then removes all items with RESET flag on.
 *
 * Both incoming lists will be destroyed while merging
 */
static List *
optionsMergeOptionValues(List *old_options, List *new_options)
{
	List	   *result = NIL;
	ListCell   *old_cell;
	ListCell   *old_prev;
	ListCell   *old_next;
	ListCell   *new_cell;
	ListCell   *new_prev;
	ListCell   *new_next;

	old_prev = NULL;

	/*
	 * First try to remove from old options list all values that exists in a
	 * new options list
	 */
	for (old_cell = list_head(old_options); old_cell; old_cell = old_next)
	{
		bool		found;
		const char *old_name;
		option_value *old_option;

		old_next = lnext(old_cell);
		old_option = (option_value *) lfirst(old_cell);
		if (old_option->status == OPTION_VALUE_STATUS_PARSED)
			old_name = old_option->gen->name;
		else
			old_name = old_option->raw_name;

		/*
		 * Try to find old_name option among new option
		 */
		found = false;
		foreach(new_cell, new_options)
		{
			option_value *new_option;
			const char *new_name;

			new_option = (option_value *) lfirst(new_cell);
			if (new_option->status == OPTION_VALUE_STATUS_PARSED)
				new_name = new_option->gen->name;
			else
				new_name = new_option->raw_name;
			if (pg_strcasecmp(new_name, old_name) == 0)
			{
				found = true;
				break;
			}
		}

		/*
		 * If found, delete old option from the list
		 */
		if (found)
		{
			old_options = list_delete_cell(old_options, old_cell, old_prev);
		}
		else
		{
			old_prev = old_cell;
		}
	}

	/*
	 * Remove from new_options all options that are for RESET. In old list all
	 * this options were already removed in previous block, so all RESET
	 * options are cleaned now
	 */
	new_prev = NULL;
	for (new_cell = list_head(new_options); new_cell; new_cell = new_next)
	{
		option_value *new_option = (option_value *) lfirst(new_cell);

		new_next = lnext(new_cell);

		if (new_option->status == OPTION_VALUE_STATUS_FOR_RESET)
			new_options = list_delete_cell(new_options, new_cell, new_prev);
		else
			new_prev = new_cell;
	}

	/*
	 * Now merge what remained of both lists
	 */
	result = list_concat(old_options, new_options);
	return result;
}

/*
 * optionsDefListValdateNamespaces
 *		Function checks that all options represented as DefList has no
 *		namespaces or have namespaces only from allowed list
 *
 * Function accept options as DefList and NULL terminated list of allowed
 * namespaces. It throws an error if not proper namespace was found.
 *
 * This function actually used only for tables with it's toast. namespace
 */
void
optionsDefListValdateNamespaces(List *defList, char **allowed_namespaces)
{
	ListCell   *cell;

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/*
		 * Checking namespace only for options that have namespaces. Options
		 * with no namespaces are always accepted
		 */
		if (def->defnamespace)
		{
			bool		found = false;
			int			i = 0;

			while (allowed_namespaces[i])
			{
				if (pg_strcasecmp(def->defnamespace,
								  allowed_namespaces[i]) == 0)
				{
					found = true;
					break;
				}
				i++;
			}
			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized parameter namespace \"%s\"",
								def->defnamespace)));
		}
	}
}

/*
 * optionsDefListFilterNamespaces
 *		Iterates over DefList, choose items with specified namespace and adds
 *		them to a result List
 *
 * This function does not destroy source DefList but does not create copies
 * of List nodes.
 * It is actually used only for tables, in order to split toast and heap
 * reloptions, so each one can be stored in on it's own pg_class record
 */
List *
optionsDefListFilterNamespaces(List *defList, char *namespace)
{
	ListCell   *cell;
	List	   *result = NIL;

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if ((!namespace && !def->defnamespace) ||
			(namespace && def->defnamespace &&
			 pg_strcasecmp(namespace, def->defnamespace) == 0))
		{
			result = lappend(result, def);
		}
	}
	return result;
}

/*
 * optionsTextArrayToDefList
 *		Convert the text-array format of reloptions into a List of DefElem.
 */
List *
optionsTextArrayToDefList(Datum options)
{
	List	   *result = NIL;
	ArrayType  *array;
	Datum	   *optiondatums;
	int			noptions;
	int			i;

	/* Nothing to do if no options */
	if (!PointerIsValid(DatumGetPointer(options)))
		return result;

	array = DatumGetArrayTypeP(options);

	deconstruct_array(array, TEXTOID, -1, false, 'i',
					  &optiondatums, NULL, &noptions);

	for (i = 0; i < noptions; i++)
	{
		char	   *s;
		char	   *p;
		Node	   *val = NULL;

		s = TextDatumGetCString(optiondatums[i]);
		p = strchr(s, '=');
		if (p)
		{
			*p++ = '\0';
			val = (Node *) makeString(pstrdup(p));
		}
		result = lappend(result, makeDefElem(pstrdup(s), val, -1));
	}

	return result;
}

/*
 * optionsParseRawValues
 *		Parses and vlaidates (if proper flag is set) option_values. As a result
 *		caller will get the list of parsed (or partly parsed) option_values
 *
 * This function is used in cases when caller gets raw values from db or
 * syntax and want to parse them.
 * This function uses option_catalog to get information about how each option
 * should be parsed.
 * If validate mode is off, function found an option that do not have proper
 * option_catalog entry, this option kept unparsed (if some garbage came from
 * the DB, we should put it back there)
 *
 * This function destroys incoming list.
 */
static List *
optionsParseRawValues(List *raw_values, options_catalog * catalog,
					  options_parse_mode mode)
{
	ListCell   *cell;
	List	   *result = NIL;
	bool	   *is_set;
	int			i;
	bool		validate = mode & OPTIONS_PARSE_MODE_VALIDATE;
	bool		for_alter = mode & OPTIONS_PARSE_MODE_FOR_ALTER;


	is_set = palloc0(sizeof(bool) * catalog->num);
	foreach(cell, raw_values)
	{
		option_value *option = (option_value *) lfirst(cell);
		bool		found = false;
		bool		skip = false;


		if (option->status == OPTION_VALUE_STATUS_PARSED)
		{
			/*
			 * This can happen while ALTER, when new values were already
			 * parsed, but old values merged from DB are still raw
			 */
			result = lappend(result, option);
			continue;
		}
		if (validate && option->namespace && (!catalog->namespace ||
				  pg_strcasecmp(catalog->namespace, option->namespace) != 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter namespace \"%s\"",
							option->namespace)));
		}

		for (i = 0; i < catalog->num; i++)
		{
			option_definition_basic *definition = catalog->definitions[i];

			if (pg_strcasecmp(option->raw_name,
							  definition->name) == 0)
			{
				/*
				 * Skip option with "ignore" flag, as it is processed
				 * somewhere else. (WITH OIDS special case)
				 */
				if (definition->flags & OPTION_DEFINITION_FLAG_IGNORE)
				{
					found = true;
					skip = true;
					break;
				}

				/*
				 * Reject option as if it was not in catalog. Needed for cases
				 * when option should have default value, but should not be
				 * changed
				 */
				if (definition->flags & OPTION_DEFINITION_FLAG_REJECT)
				{
					found = false;
					break;
				}

				if (validate && is_set[i])
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("parameter \"%s\" specified more than once",
								 option->raw_name)));
				}
				if ((for_alter) &&
					(definition->flags & OPTION_DEFINITION_FLAG_FORBID_ALTER))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						   errmsg("changing parameter \"%s\" is not allowed",
								  definition->name)));
				}
				if (option->status == OPTION_VALUE_STATUS_FOR_RESET)
				{
					/*
					 * For RESET options do not need further processing so
					 * mark it found and stop searching
					 */
					found = true;
					break;
				}
				pfree(option->raw_name);
				option->raw_name = NULL;
				option->gen = definition;
				parse_one_option(option, NULL, -1, validate);
				is_set[i] = true;
				found = true;
				break;
			}
		}
		if (!found)
		{
			if (validate)
			{
				if (option->namespace)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("unrecognized parameter \"%s.%s\"",
									option->namespace, option->raw_name)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("unrecognized parameter \"%s\"",
									option->raw_name)));
			}

			/*
			 * If we are parsing not in validate mode, then we should keep
			 * unknown node, because non-validate mode is for data that is
			 * already in the DB and should not be changed after altering
			 * another entries
			 */
		}
		if (!skip)
			result = lappend(result, option);
	}
	return result;
}

/*
 * parse_one_option
 *
 *		Subroutine for optionsParseRawValues, to parse and validate a
 *		single option's value
 */
static void
parse_one_option(option_value * option, char *text_str, int text_len,
				 bool validate)
{
	char	   *value;
	bool		parsed;

	value = option->raw_value;

	switch (option->gen->type)
	{
		case OPTION_TYPE_BOOL:
			{
				parsed = parse_bool(value, &option->values.bool_val);
				if (validate && !parsed)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid value for boolean option \"%s\": %s",
							   option->gen->name, value)));
			}
			break;
		case OPTION_TYPE_INT:
			{
				option_definition_int *optint =
				(option_definition_int *) option->gen;

				parsed = parse_int(value, &option->values.int_val, 0, NULL);
				if (validate && !parsed)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid value for integer option \"%s\": %s",
							   option->gen->name, value)));
				if (validate && (option->values.int_val < optint->min ||
								 option->values.int_val > optint->max))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						   errmsg("value %s out of bounds for option \"%s\"",
								  value, option->gen->name),
					 errdetail("Valid values are between \"%d\" and \"%d\".",
							   optint->min, optint->max)));
			}
			break;
		case OPTION_TYPE_REAL:
			{
				option_definition_real *optreal =
				(option_definition_real *) option->gen;

				parsed = parse_real(value, &option->values.real_val);
				if (validate && !parsed)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for floating point option \"%s\": %s",
									option->gen->name, value)));
				if (validate && (option->values.real_val < optreal->min ||
								 option->values.real_val > optreal->max))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						   errmsg("value %s out of bounds for option \"%s\"",
								  value, option->gen->name),
					 errdetail("Valid values are between \"%f\" and \"%f\".",
							   optreal->min, optreal->max)));
			}
			break;
		case OPTION_TYPE_ENUM:
			{
				option_definition_enum *opt_enum =
				(option_definition_enum *) option->gen;
				int			i = 0;

				parsed = false;
				while (opt_enum->allowed_values[i])
				{
					if (pg_strcasecmp(value, opt_enum->allowed_values[i]) == 0)
					{
						option->values.enum_val = i;
						parsed = true;
						break;
					}
					i++;
				}
				if (!parsed)
				{
					int			length = 0;
					char	   *str;
					char	   *ptr;

					/*
					 * Generating list of allowed values: "value1", "value2",
					 * ... "valueN"
					 */
					i = 0;
					while (opt_enum->allowed_values[i])
					{
						length += strlen(opt_enum->allowed_values[i]) + 4;
						/* +4: two quotes, one comma, one space */
						i++;
					}

					/*
					 * one byte not used for comma after the last item will be
					 * used for \0; for another byte will do -1
					 */
					str = palloc((length - 1) * sizeof(char));
					i = 0;
					ptr = str;
					while (opt_enum->allowed_values[i])
					{
						if (i != 0)
						{
							ptr[0] = ',';
							ptr[1] = ' ';
							ptr += 2;
						}
						ptr[0] = '"';
						ptr++;
						sprintf(ptr, "%s", opt_enum->allowed_values[i]);
						ptr += strlen(ptr);
						ptr[0] = '"';
						ptr++;
						i++;
					}
					*ptr = '\0';

					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for \"%s\" option",
									option->gen->name),
							 errdetail("Valid values are %s.", str)));
				}
			}
			break;
		case OPTION_TYPE_STRING:
			{
				option_definition_string *optstring =
				(option_definition_string *) option->gen;

				option->values.string_val = value;
				if (validate && optstring->validate_cb)
					(optstring->validate_cb) (value);
				parsed = true;
			}
			break;
		default:
			elog(ERROR, "unsupported reloption type %d", option->gen->type);
			parsed = true;		/* quiet compiler */
			break;
	}

	if (parsed)
		option->status = OPTION_VALUE_STATUS_PARSED;

}

/*
 * optionsAllocateBytea
 *		Allocates memory for bytea options representation
 *
 * Function allocates memory for byrea structure of an option, plus adds space
 * for values of string options. We should keep all data including string
 * values in the same memory chunk, because Cache code copies bytea option
 * data from one MemoryConext to another without knowing about it's internal
 * structure, so it would not be able to copy string values if they are outside
 * of bytea memory chunk.
 */
static void *
optionsAllocateBytea(options_catalog * catalog, List *options)
{
	Size		size;
	int			i;
	ListCell   *cell;
	int			length;
	void	   *res;

	size = catalog->struct_size;

	/* Calculate size needed to store all string values for this option */
	for (i = 0; i < catalog->num; i++)
	{
		option_definition_basic *definition = catalog->definitions[i];
		bool		found = false;
		option_value *option;

		/* Not interested in non-string options, skipping */
		if (definition->type != OPTION_TYPE_STRING)
			continue;

		/*
		 * Trying to find option_value that references definition catalog
		 * entry
		 */
		foreach(cell, options)
		{
			option = (option_value *) lfirst(cell);
			if (option->status == OPTION_VALUE_STATUS_PARSED &&
				pg_strcasecmp(option->gen->name, definition->name) == 0)
			{
				found = true;
				break;
			}
		}
		if (found)
			/* If found, it'value will be stored */
			length = strlen(option->values.string_val) + 1;
		else
			/* If not found, then there would be default value there */
		if (((option_definition_string *) definition)->default_val)
			length = strlen(
				 ((option_definition_string *) definition)->default_val) + 1;
		else
			length = 0;
		/* Add total length of all string values to basic size */
		size += length;
	}

	res = palloc0(size);
	SET_VARSIZE(res, size);
	return res;
}

/*
 * optionsValuesToBytea
 *		Converts options from List of option_values to binary bytea structure
 *
 * Convertation goes according to options_catalog: each catalog item
 * has offset value, and option value in binary mode is written to the
 * structure with that offset.
 *
 * More special case is string values. Memory for bytea structure is allocated
 * by optionsAllocateBytea which adds some more space for string values to
 * the size of original structure. All string values are copied there and
 * inside the bytea structure an offset to that value is kept.
 *
 */
static bytea *
optionsValuesToBytea(List *options, options_catalog * catalog)
{
	char	   *data;
	char	   *string_values_buffer;
	int			i;

	data = optionsAllocateBytea(catalog, options);

	/* place for string data starts right after original structure */
	string_values_buffer = data + catalog->struct_size;

	for (i = 0; i < catalog->num; i++)
	{
		option_value *found = NULL;
		ListCell   *cell;
		char	   *item_pos;
		option_definition_basic *definition = catalog->definitions[i];

		if (definition->flags & OPTION_DEFINITION_FLAG_IGNORE)
			continue;

		/* Calculate the position of the item inside the structure */
		item_pos = data + definition->struct_offset;

		/* Looking for the corresponding option from options list */
		foreach(cell, options)
		{
			option_value *option = (option_value *) lfirst(cell);

			if (option->status == OPTION_VALUE_STATUS_RAW)
				continue;		/* raw can come from db. Just ignore them then */
			Assert(option->status != OPTION_VALUE_STATUS_EMPTY);

			if (pg_strcasecmp(definition->name, option->gen->name) == 0)
			{
				found = option;
				break;
			}
		}
		/* writing to the proper position either option value or default val */
		switch (definition->type)
		{
			case OPTION_TYPE_BOOL:
				*(bool *) item_pos = found ?
					found->values.bool_val :
					((option_definition_bool *) definition)->default_val;
				break;
			case OPTION_TYPE_INT:
				*(int *) item_pos = found ?
					found->values.int_val :
					((option_definition_int *) definition)->default_val;
				break;
			case OPTION_TYPE_REAL:
				*(double *) item_pos = found ?
					found->values.real_val :
					((option_definition_real *) definition)->default_val;
				break;
			case OPTION_TYPE_ENUM:
				*(int *) item_pos = found ?
					found->values.enum_val :
					((option_definition_enum *) definition)->default_val;
				break;

			case OPTION_TYPE_STRING:
				{
					/*
					 * For string options: writing string value at the string
					 * buffer after the structure, and storing and offset to
					 * that value
					 */
					char	   *value = NULL;

					if (found)
						value = found->values.string_val;
					else
						value = ((option_definition_string *) definition)
							->default_val;
					*(int *) item_pos = value ?
						string_values_buffer - data :
						OPTION_STRING_VALUE_NOT_SET_OFFSET;
					if (value)
					{
						strcpy(string_values_buffer, value);
						string_values_buffer += strlen(value) + 1;
					}
				}
				break;
			default:
				elog(ERROR, "unsupported reloption type %d",
					 definition->type);
				break;
		}
	}
	return (void *) data;
}


/*
 * transformOptions
 *		This function is used by src/backend/commands/Xxxx in order to process
 *		new option values, merge them with existing values (in the case of
 *		ALTER command) and prepare to put them [back] into DB
 */

Datum
transformOptions(options_catalog * catalog, Datum oldOptions,
				 List *defList, options_parse_mode parse_mode)
{
	Datum		result;
	List	   *new_values;
	List	   *old_values;
	List	   *merged_values;

	/*
	 * Parse and validate New values
	 */
	new_values = optionsDefListToRawValues(defList, parse_mode);
	new_values = optionsParseRawValues(new_values, catalog,
								   parse_mode | OPTIONS_PARSE_MODE_VALIDATE);

	/*
	 * Old values exists in case of ALTER commands. Transform them to raw
	 * values and merge them with new_values, and parse it.
	 */
	if (PointerIsValid(DatumGetPointer(oldOptions)))
	{
		old_values = optionsTextArrayToRawValues(oldOptions);
		merged_values = optionsMergeOptionValues(old_values, new_values);

		/*
		 * Parse options only after merging in order not to parse options that
		 * would be removed by merging later
		 */
		merged_values = optionsParseRawValues(merged_values, catalog, 0);
	}
	else
	{
		merged_values = new_values;
	}

	/*
	 * If we have postprocess_fun function defined in catalog, then there
	 * might be some custom options checks there, with error throwing. So we
	 * should do it here to throw these errors while CREATing or ALTERing
	 * options
	 */
	if (catalog->postprocess_fun)
	{
		bytea	   *data = optionsValuesToBytea(merged_values, catalog);

		catalog->postprocess_fun(data, true);
		pfree(data);
	}

	/*
	 * Convert options to TextArray format so caller can store them into
	 * database
	 */
	result = optionsValuesToTextArray(merged_values);
	return result;
}


/*
 * optionsTextArrayToBytea
 *		A meta-function that transforms options stored as TextArray into binary
 *		(bytea) representation.
 *
 *	This function runs other transform functions that leads to the desired
 *	result in no-validation mode. This function is used by cache mechanism,
 *	in order to load and cache options when object itself is loaded and cached
 */
bytea *
optionsTextArrayToBytea(options_catalog * catalog, Datum data)
{
	List	   *values;
	bytea	   *options;

	values = optionsTextArrayToRawValues(data);
	values = optionsParseRawValues(values, catalog, 0);
	options = optionsValuesToBytea(values, catalog);

	if (catalog->postprocess_fun)
	{
		catalog->postprocess_fun(options, false);
	}
	return options;
}
