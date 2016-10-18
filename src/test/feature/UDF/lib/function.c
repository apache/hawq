#include "postgres.h"
#include "funcapi.h"
#include "tablefuncapi.h"

#include <float.h>
#include <math.h>

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "cdb/memquota.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "parser/parse_expr.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "utils/builtins.h"
#include "utils/geo_decls.h"
#include "utils/lsyscache.h"
#include "utils/resscheduler.h"


#define LDELIM '('
#define RDELIM ')'
#define NARGS  3
#define TTDUMMY_INFINITY 999999


extern Datum int44in(PG_FUNCTION_ARGS);
extern Datum int44out(PG_FUNCTION_ARGS);
extern Datum check_primary_key(PG_FUNCTION_ARGS);
extern Datum check_foreign_key(PG_FUNCTION_ARGS);
extern Datum autoinc(PG_FUNCTION_ARGS);
extern Datum funny_dup17(PG_FUNCTION_ARGS);
extern Datum ttdummy(PG_FUNCTION_ARGS);
extern Datum set_ttdummy(PG_FUNCTION_ARGS);
extern Datum c_add(PG_FUNCTION_ARGS);

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif



/* widget_in and widget_out */
typedef struct
{
  Point   center;
  double    radius;
} WIDGET;

WIDGET *widget_in(char *str);
char *widget_out(WIDGET * widget);

WIDGET *widget_in(char *str)
{
	char *p, *coord[NARGS], buf2[1000];
	int i;
	WIDGET *result;

	if (str == NULL)
	{
		return NULL;
	}

	for (i = 0, p = str; *p && i < NARGS && *p != RDELIM; p++)
	{
		if (*p == ',' || (*p == LDELIM && !i))
		{
			coord[i++] = p + 1;
		}
	}

	if (i < NARGS - 1)
	{
		return NULL;
	}

	result = (WIDGET *) palloc(sizeof(WIDGET));
	result->center.x = atof(coord[0]);
	result->center.y = atof(coord[1]);
	result->radius = atof(coord[2]);

	snprintf(buf2, sizeof(buf2), "widget_in: read (%f, %f, %f)\n",
	         result->center.x, result->center.y, result->radius);

	return result;
}


char *widget_out(WIDGET * widget)
{
	char *result;

	if (widget == NULL)
	{
		return NULL;
	}

	result = (char *) palloc(60);

	sprintf(result, "(%g,%g,%g)",
	        widget->center.x, widget->center.y, widget->radius);

	return result;
}



/* int44in and int44out */
/*
 * Type int44 has no real-world use, but the function tests use it.
 * It's a four-element vector of int4's.
 */

/*
 * int44in: converts "num num ..." to internal form
 * Note: Fills any missing positions with zeroes.
 */
PG_FUNCTION_INFO_V1(int44in);
Datum int44in(PG_FUNCTION_ARGS)
{
	char *input_string = PG_GETARG_CSTRING(0);
	int32 *result = (int32 *) palloc(4 * sizeof(int32));
	int i;

	i = sscanf(input_string,
	           "%d, %d, %d, %d",
	           &result[0],
	           &result[1],
	           &result[2],
	           &result[3]);

	while (i < 4)
	{
		result[i++] = 0;
	}

	PG_RETURN_POINTER(result);
}

/*
 * int44out: converts internal form to "num num ..."
 */
PG_FUNCTION_INFO_V1(int44out);
Datum int44out(PG_FUNCTION_ARGS)
{
	int32 *an_array = (int32 *) PG_GETARG_POINTER(0);
	/* Allow 14 digits sign */
	char *result = (char *) palloc(16 * 4);
	int i;
	char *walk;

	walk = result;
	for (i = 0; i < 4; i++)
	{
		pg_ltoa(an_array[i], walk);
		while (*++walk != '\0')
			;
		*walk++ = ' ';
	}
	*--walk = '\0';
	PG_RETURN_CSTRING(result);
}



/* check_primary_key, check_foreign_key and helper function find_plan */
typedef struct
{
	char	   *ident;
	int			nplans;
	void	  **splan;
}	EPlan;

static EPlan *FPlans = NULL;
static int	nFPlans = 0;
static EPlan *PPlans = NULL;
static int	nPPlans = 0;

static EPlan *find_plan(char *ident, EPlan ** eplan, int *nplans);

/*
 * check_primary_key () -- check that key in tuple being inserted/updated
 *			 references existing tuple in "primary" table.
 * Though it's called without args You have to specify referenced
 * table/keys while creating trigger:  key field names in triggered table,
 * referenced table name, referenced key field names:
 * EXECUTE PROCEDURE
 * check_primary_key ('Fkey1', 'Fkey2', 'Ptable', 'Pkey1', 'Pkey2').
 */
PG_FUNCTION_INFO_V1(check_primary_key);
Datum check_primary_key(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger    *trigger;		/* to get trigger name */
	int			nargs;			/* # of args specified in CREATE TRIGGER */
	char	  **args;			/* arguments: column names and table name */
	int			nkeys;			/* # of key columns (= nargs / 2) */
	Datum	   *kvals;			/* key values */
	char	   *relname;		/* referenced relation name */
	Relation	rel;			/* triggered relation */
	HeapTuple	tuple = NULL;	/* tuple to return */
	TupleDesc	tupdesc;		/* tuple description */
	EPlan	   *plan;			/* prepared plan */
	Oid		   *argtypes = NULL;	/* key types to prepare execution plan */
	bool		isnull;			/* to know is some column NULL or not */
	char		ident[2 * NAMEDATALEN]; /* to identify myself */
	int			ret;
	int			i;

#ifdef	DEBUG_QUERY
	elog(DEBUG4, "check_primary_key: Enter Function");
#endif

	/*
	 * Some checks first...
	 */

	/* Called by trigger manager ? */
	if (!CALLED_AS_TRIGGER(fcinfo))
		/* internal error */
		elog(ERROR, "check_primary_key: not fired by trigger manager");

	/* Should be called for ROW trigger */
	if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		/* internal error */
		elog(ERROR, "check_primary_key: can't process STATEMENT events");

	/* If INSERTion then must check Tuple to being inserted */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		tuple = trigdata->tg_trigtuple;

	/* Not should be called for DELETE */
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		/* internal error */
		elog(ERROR, "check_primary_key: can't process DELETE events");

	/* If UPDATion the must check new Tuple, not old one */
	else
		tuple = trigdata->tg_newtuple;

	trigger = trigdata->tg_trigger;
	nargs = trigger->tgnargs;
	args = trigger->tgargs;

	if (nargs % 2 != 1)			/* odd number of arguments! */
		/* internal error */
		elog(ERROR, "check_primary_key: odd number of arguments should be specified");

	nkeys = nargs / 2;
	relname = args[nkeys];
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;

	/* Connect to SPI manager */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "check_primary_key: SPI_connect returned %d", ret);

	/*
	 * We use SPI plan preparation feature, so allocate space to place key
	 * values.
	 */
	kvals = (Datum *) palloc(nkeys * sizeof(Datum));

	/*
	 * Construct ident string as TriggerName $ TriggeredRelationId and try to
	 * find prepared execution plan.
	 */
	snprintf(ident, sizeof(ident), "%s$%u", trigger->tgname, rel->rd_id);
	plan = find_plan(ident, &PPlans, &nPPlans);

	/* if there is no plan then allocate argtypes for preparation */
	if (plan->nplans <= 0)
		argtypes = (Oid *) palloc(nkeys * sizeof(Oid));

	/* For each column in key ... */
	for (i = 0; i < nkeys; i++)
	{
		/* get index of column in tuple */
		int			fnumber = SPI_fnumber(tupdesc, args[i]);

		/* Bad guys may give us un-existing column in CREATE TRIGGER */
		if (fnumber < 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("there is no attribute \"%s\" in relation \"%s\"",
							args[i], SPI_getrelname(rel))));

		/* Well, get binary (in internal format) value of column */
		kvals[i] = SPI_getbinval(tuple, tupdesc, fnumber, &isnull);

		/*
		 * If it's NULL then nothing to do! DON'T FORGET call SPI_finish ()!
		 * DON'T FORGET return tuple! Executor inserts tuple you're returning!
		 * If you return NULL then nothing will be inserted!
		 */
		if (isnull)
		{
			SPI_finish();
			return PointerGetDatum(tuple);
		}

		if (plan->nplans <= 0)	/* Get typeId of column */
			argtypes[i] = SPI_gettypeid(tupdesc, fnumber);
	}

	/*
	 * If we have to prepare plan ...
	 */
	if (plan->nplans <= 0)
	{
		void	   *pplan;
		char		sql[8192];

		/*
		 * Construct query: SELECT 1 FROM _referenced_relation_ WHERE Pkey1 =
		 * $1 [AND Pkey2 = $2 [...]]
		 */
		snprintf(sql, sizeof(sql), "select 1 from %s where ", relname);
		for (i = 0; i < nkeys; i++)
		{
			snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), "%s = $%d %s",
				  args[i + nkeys + 1], i + 1, (i < nkeys - 1) ? "and " : "");
		}

		/* Prepare plan for query */
		pplan = SPI_prepare(sql, nkeys, argtypes);
		if (pplan == NULL)
			/* internal error */
			elog(ERROR, "check_primary_key: SPI_prepare returned %d", SPI_result);

		/*
		 * Remember that SPI_prepare places plan in current memory context -
		 * so, we have to save plan in Top memory context for latter use.
		 */
		pplan = SPI_saveplan(pplan);
		if (pplan == NULL)
			/* internal error */
			elog(ERROR, "check_primary_key: SPI_saveplan returned %d", SPI_result);
		plan->splan = (void **) malloc(sizeof(void *));
		*(plan->splan) = pplan;
		plan->nplans = 1;
	}

	/*
	 * Ok, execute prepared plan.
	 */
	ret = SPI_execp(*(plan->splan), kvals, NULL, 1);
	/* we have no NULLs - so we pass   ^^^^   here */

	if (ret < 0)
		/* internal error */
		elog(ERROR, "check_primary_key: SPI_execp returned %d", ret);

	/*
	 * If there are no tuples returned by SELECT then ...
	 */
	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("tuple references non-existent key"),
				 errdetail("Trigger \"%s\" found tuple referencing non-existent key in \"%s\".", trigger->tgname, relname)));

	SPI_finish();

	return PointerGetDatum(tuple);
}


/*
 * check_foreign_key () -- check that key in tuple being deleted/updated
 *			 is not referenced by tuples in "foreign" table(s).
 * Though it's called without args You have to specify (while creating trigger):
 * number of references, action to do if key referenced
 * ('restrict' | 'setnull' | 'cascade'), key field names in triggered
 * ("primary") table and referencing table(s)/keys:
 * EXECUTE PROCEDURE
 * check_foreign_key (2, 'restrict', 'Pkey1', 'Pkey2',
 * 'Ftable1', 'Fkey11', 'Fkey12', 'Ftable2', 'Fkey21', 'Fkey22').
 */
PG_FUNCTION_INFO_V1(check_foreign_key);
Datum check_foreign_key(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger    *trigger;		/* to get trigger name */
	int			nargs;			/* # of args specified in CREATE TRIGGER */
	char	  **args;			/* arguments: as described above */
	char	  **args_temp;
	int			nrefs;			/* number of references (== # of plans) */
	char		action;			/* 'R'estrict | 'S'etnull | 'C'ascade */
	int			nkeys;			/* # of key columns */
	Datum	   *kvals;			/* key values */
	char	   *relname;		/* referencing relation name */
	Relation	rel;			/* triggered relation */
	HeapTuple	trigtuple = NULL;		/* tuple to being changed */
	HeapTuple	newtuple = NULL;	/* tuple to return */
	TupleDesc	tupdesc;		/* tuple description */
	EPlan	   *plan;			/* prepared plan(s) */
	Oid		   *argtypes = NULL;	/* key types to prepare execution plan */
	bool		isnull;			/* to know is some column NULL or not */
	bool		isequal = true; /* are keys in both tuples equal (in UPDATE) */
	char		ident[2 * NAMEDATALEN]; /* to identify myself */
	int			is_update = 0;
	int			ret;
	int			i,
				r;

#ifdef DEBUG_QUERY
	elog(DEBUG4, "check_foreign_key: Enter Function");
#endif

	/*
	 * Some checks first...
	 */

	/* Called by trigger manager ? */
	if (!CALLED_AS_TRIGGER(fcinfo))
		/* internal error */
		elog(ERROR, "check_foreign_key: not fired by trigger manager");

	/* Should be called for ROW trigger */
	if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		/* internal error */
		elog(ERROR, "check_foreign_key: can't process STATEMENT events");

	/* Not should be called for INSERT */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		/* internal error */
		elog(ERROR, "check_foreign_key: can't process INSERT events");

	/* Have to check tg_trigtuple - tuple being deleted */
	trigtuple = trigdata->tg_trigtuple;

	/*
	 * But if this is UPDATE then we have to return tg_newtuple. Also, if key
	 * in tg_newtuple is the same as in tg_trigtuple then nothing to do.
	 */
	is_update = 0;
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		newtuple = trigdata->tg_newtuple;
		is_update = 1;
	}
	trigger = trigdata->tg_trigger;
	nargs = trigger->tgnargs;
	args = trigger->tgargs;

	if (nargs < 5)				/* nrefs, action, key, Relation, key - at
								 * least */
		/* internal error */
		elog(ERROR, "check_foreign_key: too short %d (< 5) list of arguments", nargs);

	nrefs = pg_atoi(args[0], sizeof(int), 0);
	if (nrefs < 1)
		/* internal error */
		elog(ERROR, "check_foreign_key: %d (< 1) number of references specified", nrefs);
	action = tolower((unsigned char) *(args[1]));
	if (action != 'r' && action != 'c' && action != 's')
		/* internal error */
		elog(ERROR, "check_foreign_key: invalid action %s", args[1]);
	nargs -= 2;
	args += 2;
	nkeys = (nargs - nrefs) / (nrefs + 1);
	if (nkeys <= 0 || nargs != (nrefs + nkeys * (nrefs + 1)))
		/* internal error */
		elog(ERROR, "check_foreign_key: invalid number of arguments %d for %d references",
			 nargs + 2, nrefs);

	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;

	/* Connect to SPI manager */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "check_foreign_key: SPI_connect returned %d", ret);

	/*
	 * We use SPI plan preparation feature, so allocate space to place key
	 * values.
	 */
	kvals = (Datum *) palloc(nkeys * sizeof(Datum));

	/*
	 * Construct ident string as TriggerName $ TriggeredRelationId and try to
	 * find prepared execution plan(s).
	 */
	snprintf(ident, sizeof(ident), "%s$%u", trigger->tgname, rel->rd_id);
	plan = find_plan(ident, &FPlans, &nFPlans);

	/* if there is no plan(s) then allocate argtypes for preparation */
	if (plan->nplans <= 0)
		argtypes = (Oid *) palloc(nkeys * sizeof(Oid));

	/*
	 * else - check that we have exactly nrefs plan(s) ready
	 */
	else if (plan->nplans != nrefs)
		/* internal error */
		elog(ERROR, "%s: check_foreign_key: # of plans changed in meantime",
			 trigger->tgname);

	/* For each column in key ... */
	for (i = 0; i < nkeys; i++)
	{
		/* get index of column in tuple */
		int			fnumber = SPI_fnumber(tupdesc, args[i]);

		/* Bad guys may give us un-existing column in CREATE TRIGGER */
		if (fnumber < 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("there is no attribute \"%s\" in relation \"%s\"",
							args[i], SPI_getrelname(rel))));

		/* Well, get binary (in internal format) value of column */
		kvals[i] = SPI_getbinval(trigtuple, tupdesc, fnumber, &isnull);

		/*
		 * If it's NULL then nothing to do! DON'T FORGET call SPI_finish ()!
		 * DON'T FORGET return tuple! Executor inserts tuple you're returning!
		 * If you return NULL then nothing will be inserted!
		 */
		if (isnull)
		{
			SPI_finish();
			return PointerGetDatum((newtuple == NULL) ? trigtuple : newtuple);
		}

		/*
		 * If UPDATE then get column value from new tuple being inserted and
		 * compare is this the same as old one. For the moment we use string
		 * presentation of values...
		 */
		if (newtuple != NULL)
		{
			char	   *oldval = SPI_getvalue(trigtuple, tupdesc, fnumber);
			char	   *newval;

			/* this shouldn't happen! SPI_ERROR_NOOUTFUNC ? */
			if (oldval == NULL)
				/* internal error */
				elog(ERROR, "check_foreign_key: SPI_getvalue returned %d", SPI_result);
			newval = SPI_getvalue(newtuple, tupdesc, fnumber);
			if (newval == NULL || strcmp(oldval, newval) != 0)
				isequal = false;
		}

		if (plan->nplans <= 0)	/* Get typeId of column */
			argtypes[i] = SPI_gettypeid(tupdesc, fnumber);
	}
	args_temp = args;
	nargs -= nkeys;
	args += nkeys;

	/*
	 * If we have to prepare plans ...
	 */
	if (plan->nplans <= 0)
	{
		void	   *pplan;
		char		sql[8192];
		char	  **args2 = args;

		plan->splan = (void **) malloc(nrefs * sizeof(void *));

		for (r = 0; r < nrefs; r++)
		{
			relname = args2[0];

			/*---------
			 * For 'R'estrict action we construct SELECT query:
			 *
			 *	SELECT 1
			 *	FROM _referencing_relation_
			 *	WHERE Fkey1 = $1 [AND Fkey2 = $2 [...]]
			 *
			 *	to check is tuple referenced or not.
			 *---------
			 */
			if (action == 'r')

				snprintf(sql, sizeof(sql), "select 1 from %s where ", relname);

			/*---------
			 * For 'C'ascade action we construct DELETE query
			 *
			 *	DELETE
			 *	FROM _referencing_relation_
			 *	WHERE Fkey1 = $1 [AND Fkey2 = $2 [...]]
			 *
			 * to delete all referencing tuples.
			 *---------
			 */

			/*
			 * Max : Cascade with UPDATE query i create update query that
			 * updates new key values in referenced tables
			 */


			else if (action == 'c')
			{
				if (is_update == 1)
				{
					int			fn;
					char	   *nv;
					int			k;

					snprintf(sql, sizeof(sql), "update %s set ", relname);
					for (k = 1; k <= nkeys; k++)
					{
						int			is_char_type = 0;
						char	   *type;

						fn = SPI_fnumber(tupdesc, args_temp[k - 1]);
						nv = SPI_getvalue(newtuple, tupdesc, fn);
						type = SPI_gettype(tupdesc, fn);

						if ((strcmp(type, "text") && strcmp(type, "varchar") &&
							 strcmp(type, "char") && strcmp(type, "bpchar") &&
							 strcmp(type, "date") && strcmp(type, "timestamp")) == 0)
							is_char_type = 1;
#ifdef	DEBUG_QUERY
						elog(DEBUG4, "check_foreign_key Debug value %s type %s %d",
							 nv, type, is_char_type);
#endif

						/*
						 * is_char_type =1 i set ' ' for define a new value
						 */
						snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql),
								 " %s = %s%s%s %s ",
								 args2[k], (is_char_type > 0) ? "'" : "",
								 nv, (is_char_type > 0) ? "'" : "", (k < nkeys) ? ", " : "");
						is_char_type = 0;
					}
					strcat(sql, " where ");

				}
				else
					/* DELETE */
					snprintf(sql, sizeof(sql), "delete from %s where ", relname);

			}

			/*
			 * For 'S'etnull action we construct UPDATE query - UPDATE
			 * _referencing_relation_ SET Fkey1 null [, Fkey2 null [...]]
			 * WHERE Fkey1 = $1 [AND Fkey2 = $2 [...]] - to set key columns in
			 * all referencing tuples to NULL.
			 */
			else if (action == 's')
			{
				snprintf(sql, sizeof(sql), "update %s set ", relname);
				for (i = 1; i <= nkeys; i++)
				{
					snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql),
							 "%s = null%s",
							 args2[i], (i < nkeys) ? ", " : "");
				}
				strcat(sql, " where ");
			}

			/* Construct WHERE qual */
			for (i = 1; i <= nkeys; i++)
			{
				snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), "%s = $%d %s",
						 args2[i], i, (i < nkeys) ? "and " : "");
			}

			/* Prepare plan for query */
			pplan = SPI_prepare(sql, nkeys, argtypes);
			if (pplan == NULL)
				/* internal error */
				elog(ERROR, "check_foreign_key: SPI_prepare returned %d", SPI_result);

			/*
			 * Remember that SPI_prepare places plan in current memory context
			 * - so, we have to save plan in Top memory context for latter
			 * use.
			 */
			pplan = SPI_saveplan(pplan);
			if (pplan == NULL)
				/* internal error */
				elog(ERROR, "check_foreign_key: SPI_saveplan returned %d", SPI_result);

			plan->splan[r] = pplan;

			args2 += nkeys + 1; /* to the next relation */
		}
		plan->nplans = nrefs;
#ifdef	DEBUG_QUERY
		elog(DEBUG4, "check_foreign_key Debug Query is :  %s ", sql);
#endif
	}

	/*
	 * If UPDATE and key is not changed ...
	 */
	if (newtuple != NULL && isequal)
	{
		SPI_finish();
		return PointerGetDatum(newtuple);
	}

	/*
	 * Ok, execute prepared plan(s).
	 */
	for (r = 0; r < nrefs; r++)
	{
		/*
		 * For 'R'estrict we may to execute plan for one tuple only, for other
		 * actions - for all tuples.
		 */
		int			tcount = (action == 'r') ? 1 : 0;

		relname = args[0];

		snprintf(ident, sizeof(ident), "%s$%u", trigger->tgname, rel->rd_id);
		plan = find_plan(ident, &FPlans, &nFPlans);
		ret = SPI_execp(plan->splan[r], kvals, NULL, tcount);
		/* we have no NULLs - so we pass   ^^^^  here */

		if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
					 errmsg("SPI_execp returned %d", ret)));

		/* If action is 'R'estrict ... */
		if (action == 'r')
		{
			/* If there is tuple returned by SELECT then ... */
			if (SPI_processed > 0)
				ereport(ERROR,
						(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
						 errmsg("\"%s\": tuple is referenced in \"%s\"",
								trigger->tgname, relname)));
		}
		else
		{
#ifdef REFINT_VERBOSE
			elog(NOTICE, "%s: %d tuple(s) of %s are %s",
				 trigger->tgname, SPI_processed, relname,
				 (action == 'c') ? "deleted" : "set to null");
#endif
		}
		args += nkeys + 1;		/* to the next relation */
	}

	SPI_finish();

	return PointerGetDatum((newtuple == NULL) ? trigtuple : newtuple);
}


static EPlan *find_plan(char *ident, EPlan ** eplan, int *nplans)
{
	EPlan	   *newp;
	int			i;

	if (*nplans > 0)
	{
		for (i = 0; i < *nplans; i++)
		{
			if (strcmp((*eplan)[i].ident, ident) == 0)
				break;
		}
		if (i != *nplans)
			return (*eplan + i);
		*eplan = (EPlan *) realloc(*eplan, (i + 1) * sizeof(EPlan));
		newp = *eplan + i;
	}
	else
	{
		newp = *eplan = (EPlan *) malloc(sizeof(EPlan));
		(*nplans) = i = 0;
	}

	newp->ident = (char *) malloc(strlen(ident) + 1);
	strcpy(newp->ident, ident);
	newp->nplans = 0;
	newp->splan = NULL;
	(*nplans)++;

	return (newp);
}



/* autoinc */
PG_FUNCTION_INFO_V1(autoinc);
Datum autoinc(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger    *trigger;		/* to get trigger name */
	int			nargs;			/* # of arguments */
	int		   *chattrs;		/* attnums of attributes to change */
	int			chnattrs = 0;	/* # of above */
	Datum	   *newvals;		/* vals of above */
	char	  **args;			/* arguments */
	char	   *relname;		/* triggered relation name */
	Relation	rel;			/* triggered relation */
	HeapTuple	rettuple = NULL;
	TupleDesc	tupdesc;		/* tuple description */
	bool		isnull;
	int			i;

	if (!CALLED_AS_TRIGGER(fcinfo))
		/* internal error */
		elog(ERROR, "not fired by trigger manager");
	if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		/* internal error */
		elog(ERROR, "can't process STATEMENT events");
	if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		/* internal error */
		elog(ERROR, "must be fired before event");

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		rettuple = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else
		/* internal error */
		elog(ERROR, "can't process DELETE events");

	rel = trigdata->tg_relation;
	relname = SPI_getrelname(rel);

	trigger = trigdata->tg_trigger;

	nargs = trigger->tgnargs;
	if (nargs <= 0 || nargs % 2 != 0)
		/* internal error */
		elog(ERROR, "autoinc (%s): even number gt 0 of arguments was expected", relname);

	args = trigger->tgargs;
	tupdesc = rel->rd_att;

	chattrs = (int *) palloc(nargs / 2 * sizeof(int));
	newvals = (Datum *) palloc(nargs / 2 * sizeof(Datum));

	for (i = 0; i < nargs;)
	{
		int			attnum = SPI_fnumber(tupdesc, args[i]);
		int32		val;
		Datum		seqname;

		if (attnum < 0)
			ereport(ERROR,
					(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
					 errmsg("\"%s\" has no attribute \"%s\"",
							relname, args[i])));

		if (SPI_gettypeid(tupdesc, attnum) != INT4OID)
			ereport(ERROR,
					(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
					 errmsg("attribute \"%s\" of \"%s\" must be type INT4",
							args[i], relname)));

		val = DatumGetInt32(SPI_getbinval(rettuple, tupdesc, attnum, &isnull));

		if (!isnull && val != 0)
		{
			i += 2;
			continue;
		}

		i++;
		chattrs[chnattrs] = attnum;
		seqname = DirectFunctionCall1(textin,
									  CStringGetDatum(args[i]));
		newvals[chnattrs] = DirectFunctionCall1(nextval, seqname);
		/* nextval now returns int64; coerce down to int32 */
		newvals[chnattrs] = Int32GetDatum((int32) DatumGetInt64(newvals[chnattrs]));
		if (DatumGetInt32(newvals[chnattrs]) == 0)
		{
			newvals[chnattrs] = DirectFunctionCall1(nextval, seqname);
			newvals[chnattrs] = Int32GetDatum((int32) DatumGetInt64(newvals[chnattrs]));
		}
		pfree(DatumGetTextP(seqname));
		chnattrs++;
		i++;
	}

	if (chnattrs > 0)
	{
		rettuple = SPI_modifytuple(rel, rettuple, chnattrs, chattrs, newvals, NULL);
		if (rettuple == NULL)
			/* internal error */
			elog(ERROR, "autoinc (%s): %d returned by SPI_modifytuple",
				 relname, SPI_result);
	}

	pfree(relname);
	pfree(chattrs);
	pfree(newvals);

	return PointerGetDatum(rettuple);
}



/* funny_dup17 */
static TransactionId fd17b_xid = InvalidTransactionId;
static TransactionId fd17a_xid = InvalidTransactionId;
static int	fd17b_level = 0;
static int	fd17a_level = 0;
static bool fd17b_recursion = true;
static bool fd17a_recursion = true;

PG_FUNCTION_INFO_V1(funny_dup17);
Datum funny_dup17(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	TransactionId *xid;
	int		   *level;
	bool	   *recursion;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	char	   *query,
			   *fieldval,
			   *fieldtype;
	char	   *when;
	int			inserted;
	int			selected = 0;
	int			ret;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "funny_dup17: not fired by trigger manager");

	tuple = trigdata->tg_trigtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
	{
		xid = &fd17b_xid;
		level = &fd17b_level;
		recursion = &fd17b_recursion;
		when = "BEFORE";
	}
	else
	{
		xid = &fd17a_xid;
		level = &fd17a_level;
		recursion = &fd17a_recursion;
		when = "AFTER ";
	}

	if (!TransactionIdIsCurrentTransactionId(*xid))
	{
		*xid = GetCurrentTransactionId();
		*level = 0;
		*recursion = true;
	}

	if (*level == 17)
	{
		*recursion = false;
		return PointerGetDatum(tuple);
	}

	if (!(*recursion))
		return PointerGetDatum(tuple);

	(*level)++;

	SPI_connect();

	fieldval = SPI_getvalue(tuple, tupdesc, 1);
	fieldtype = SPI_gettype(tupdesc, 1);

	query = (char *) palloc(100 + NAMEDATALEN * 3 +
							strlen(fieldval) + strlen(fieldtype));

	sprintf(query, "insert into %s select * from %s where %s = '%s'::%s",
			SPI_getrelname(rel), SPI_getrelname(rel),
			SPI_fname(tupdesc, 1),
			fieldval, fieldtype);

	if ((ret = SPI_exec(query, 0)) < 0)
		elog(ERROR, "funny_dup17 (fired %s) on level %3d: SPI_exec (insert ...) returned %d",
			 when, *level, ret);

	inserted = SPI_processed;

	sprintf(query, "select count (*) from %s where %s = '%s'::%s",
			SPI_getrelname(rel),
			SPI_fname(tupdesc, 1),
			fieldval, fieldtype);

	if ((ret = SPI_exec(query, 0)) < 0)
		elog(ERROR, "funny_dup17 (fired %s) on level %3d: SPI_exec (select ...) returned %d",
			 when, *level, ret);

	if (SPI_processed > 0)
	{
		selected = DatumGetInt32(DirectFunctionCall1(int4in,
												CStringGetDatum(SPI_getvalue(
													   SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc,
																			 1
																		))));
	}

	elog(DEBUG4, "funny_dup17 (fired %s) on level %3d: %d/%d tuples inserted/selected",
		 when, *level, inserted, selected);

	SPI_finish();

	(*level)--;

	if (*level == 0)
		*xid = InvalidTransactionId;

	return PointerGetDatum(tuple);
}



/* ttdummy and set_ttdummy */
static SPIPlanPtr splan = NULL;
static bool ttoff = false;

PG_FUNCTION_INFO_V1(ttdummy);
Datum ttdummy(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger    *trigger;		/* to get trigger name */
	char	  **args;			/* arguments */
	int			attnum[2];		/* fnumbers of start/stop columns */
	Datum		oldon,
				oldoff;
	Datum		newon,
				newoff;
	Datum	   *cvals;			/* column values */
	char	   *cnulls;			/* column nulls */
	char	   *relname;		/* triggered relation name */
	Relation	rel;			/* triggered relation */
	HeapTuple	trigtuple;
	HeapTuple	newtuple = NULL;
	HeapTuple	rettuple;
	TupleDesc	tupdesc;		/* tuple description */
	int			natts;			/* # of attributes */
	bool		isnull;			/* to know is some column NULL or not */
	int			ret;
	int			i;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "ttdummy: not fired by trigger manager");
	if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		elog(ERROR, "ttdummy: cannot process STATEMENT events");
	if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		elog(ERROR, "ttdummy: must be fired before event");
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		elog(ERROR, "ttdummy: cannot process INSERT event");
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		newtuple = trigdata->tg_newtuple;

	trigtuple = trigdata->tg_trigtuple;

	rel = trigdata->tg_relation;
	relname = SPI_getrelname(rel);

	/* check if TT is OFF for this relation */
	if (ttoff)					/* OFF - nothing to do */
	{
		pfree(relname);
		return PointerGetDatum((newtuple != NULL) ? newtuple : trigtuple);
	}

	trigger = trigdata->tg_trigger;

	if (trigger->tgnargs != 2)
		elog(ERROR, "ttdummy (%s): invalid (!= 2) number of arguments %d",
			 relname, trigger->tgnargs);

	args = trigger->tgargs;
	tupdesc = rel->rd_att;
	natts = tupdesc->natts;

	for (i = 0; i < 2; i++)
	{
		attnum[i] = SPI_fnumber(tupdesc, args[i]);
		if (attnum[i] < 0)
			elog(ERROR, "ttdummy (%s): there is no attribute %s", relname, args[i]);
		if (SPI_gettypeid(tupdesc, attnum[i]) != INT4OID)
			elog(ERROR, "ttdummy (%s): attributes %s and %s must be of abstime type",
				 relname, args[0], args[1]);
	}

	oldon = SPI_getbinval(trigtuple, tupdesc, attnum[0], &isnull);
	if (isnull)
		elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[0]);

	oldoff = SPI_getbinval(trigtuple, tupdesc, attnum[1], &isnull);
	if (isnull)
		elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[1]);

	if (newtuple != NULL)		/* UPDATE */
	{
		newon = SPI_getbinval(newtuple, tupdesc, attnum[0], &isnull);
		if (isnull)
			elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[0]);
		newoff = SPI_getbinval(newtuple, tupdesc, attnum[1], &isnull);
		if (isnull)
			elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[1]);

		if (oldon != newon || oldoff != newoff)
			elog(ERROR, "ttdummy (%s): you cannot change %s and/or %s columns (use set_ttdummy)",
				 relname, args[0], args[1]);

		if (newoff != TTDUMMY_INFINITY)
		{
			pfree(relname);		/* allocated in upper executor context */
			return PointerGetDatum(NULL);
		}
	}
	else if (oldoff != TTDUMMY_INFINITY)		/* DELETE */
	{
		pfree(relname);
		return PointerGetDatum(NULL);
	}

	newoff = DirectFunctionCall1(nextval, CStringGetTextDatum("ttdummy_seq"));
		/* nextval now returns int64; coerce down to int32 */
		newoff = Int32GetDatum((int32) DatumGetInt64(newoff));

	/* Connect to SPI manager */
	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "ttdummy (%s): SPI_connect returned %d", relname, ret);

	/* Fetch tuple values and nulls */
	cvals = (Datum *) palloc(natts * sizeof(Datum));
	cnulls = (char *) palloc(natts * sizeof(char));
	for (i = 0; i < natts; i++)
	{
		cvals[i] = SPI_getbinval((newtuple != NULL) ? newtuple : trigtuple,
								 tupdesc, i + 1, &isnull);
		cnulls[i] = (isnull) ? 'n' : ' ';
	}

	/* change date column(s) */
	if (newtuple)				/* UPDATE */
	{
		cvals[attnum[0] - 1] = newoff;	/* start_date eq current date */
		cnulls[attnum[0] - 1] = ' ';
		cvals[attnum[1] - 1] = TTDUMMY_INFINITY;		/* stop_date eq INFINITY */
		cnulls[attnum[1] - 1] = ' ';
	}
	else
		/* DELETE */
	{
		cvals[attnum[1] - 1] = newoff;	/* stop_date eq current date */
		cnulls[attnum[1] - 1] = ' ';
	}

	/* if there is no plan ... */
	if (splan == NULL)
	{
		SPIPlanPtr	pplan;
		Oid		   *ctypes;
		char	   *query;

		/* allocate space in preparation */
		ctypes = (Oid *) palloc(natts * sizeof(Oid));
		query = (char *) palloc(100 + 16 * natts);

		/*
		 * Construct query: INSERT INTO _relation_ VALUES ($1, ...)
		 */
		sprintf(query, "INSERT INTO %s VALUES (", relname);
		for (i = 1; i <= natts; i++)
		{
			sprintf(query + strlen(query), "$%d%s",
					i, (i < natts) ? ", " : ")");
			ctypes[i - 1] = SPI_gettypeid(tupdesc, i);
		}

		/* Prepare plan for query */
		pplan = SPI_prepare(query, natts, ctypes);
		if (pplan == NULL)
			elog(ERROR, "ttdummy (%s): SPI_prepare returned %d", relname, SPI_result);

		pplan = SPI_saveplan(pplan);
		if (pplan == NULL)
			elog(ERROR, "ttdummy (%s): SPI_saveplan returned %d", relname, SPI_result);

		splan = pplan;
	}

	ret = SPI_execp(splan, cvals, cnulls, 0);

	if (ret < 0)
		elog(ERROR, "ttdummy (%s): SPI_execp returned %d", relname, ret);

	/* Tuple to return to upper Executor ... */
	if (newtuple)				/* UPDATE */
	{
		HeapTuple	tmptuple;

		tmptuple = SPI_copytuple(trigtuple);
		rettuple = SPI_modifytuple(rel, tmptuple, 1, &(attnum[1]), &newoff, NULL);
		SPI_freetuple(tmptuple);
	}
	else
		/* DELETE */
		rettuple = trigtuple;

	SPI_finish();				/* don't forget say Bye to SPI mgr */

	pfree(relname);

	return PointerGetDatum(rettuple);
}


PG_FUNCTION_INFO_V1(set_ttdummy);
Datum set_ttdummy(PG_FUNCTION_ARGS)
{
	int32		on = PG_GETARG_INT32(0);

	if (ttoff)					/* OFF currently */
	{
		if (on == 0)
			PG_RETURN_INT32(0);

		/* turn ON */
		ttoff = false;
		PG_RETURN_INT32(0);
	}

	/* ON currently */
	if (on != 0)
		PG_RETURN_INT32(1);

	/* turn OFF */
	ttoff = true;

	PG_RETURN_INT32(1);
}

PG_FUNCTION_INFO_V1(c_add);
Datum c_add(PG_FUNCTION_ARGS)
{
	int32 x = PG_GETARG_INT32(0);
	int32 y = PG_GETARG_INT32(1);

	PG_RETURN_INT32(x+y);
}

