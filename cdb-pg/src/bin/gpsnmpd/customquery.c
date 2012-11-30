#include "gpsnmpd.h"
#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <net-snmp/library/snmp_assert.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "customquery.h"
#include "query_reader.h"

/* TODO: if this works, move it to pgsnmpd.h */
#define CQ_MAX_LEN   255
/* TODO: change this when we support multiple connections */
const int pgsnmpdConnID = 1;
/* TODO: change this when we support row slices in rdbmsDbTable */
const int rdbmsDbIndex = 1;

char *custom_query_config_file = NULL;

/* This makes it easier to account for rdbmsDbIndex and pgsnmpdConnID */
const int extra_idx_count = 2;

/*
 * Note: this code uses net-snmp containers. See
 * http://net-snmp.sourceforge.net/wiki/index.php/Containers for a helpful
 * reference on the subject
 */

/* TODO: Replace this with reading something meaningful from a config file */
typedef struct example_query
{
	char table_name[25];
	char query_string[300];
	oid query_oid[MAX_OID_LEN];
	size_t oid_len;
	int index_cols; /* Count of columns from the query that are indexes. Index columns should be the first ones returned */
	int min_colnum; /* Starting column number, should be greater than 0 */
} example_query;

const int q_len = 2;

example_query
		q[] =
		{
				/* {
				 "pg_database",
				 "SELECT oid, encoding, datdba, * FROM pg_database",
				 {1, 3, 6, 1, 4, 1, 27645, 1, 3, 1},
				 10,
				 1,
				 1
				 },
				 {
				 "random",
				 "SELECT 1, 2, floor(100 * random()) union select 4, floor(random() * 10), 5",
				 {1, 3, 6, 1, 4, 1, 27645, 1, 3, 2},
				 10,
				 1,
				 1
				 },
				 {
				 "pg_class",
				 "SELECT oid, reltuples, reltoastrelid, 'test string', * FROM pg_class",
				 {1, 3, 6, 1, 4, 1, 27645, 1, 3, 3},
				 10,
				 1,
				 1
				 },
				 {
				 "test",
				 "SELECT 'test', 't'::boolean, 14",
				 {1, 3, 6, 1, 4, 1, 27646, 1, 1},
				 9,
				 1,
				 1
				 },  */
				{
						"easytest",
						"SELECT 1, 'test', 't'::boolean, random() UNION SELECT 2, 'test2', 'f'::boolean, random()",
						{ 1, 3, 6, 1, 4, 1, 27646, 1, 2 }, 9, 1, 1 },
				/*{
				 "ifTable",
				 "SELECT 1, 'Desc', 24, 1, 13, 'Physical Address' UNION SELECT 2, 'Desc2', 25, 2, 14, '2 Physical Address'",
				 {1, 3, 6, 1, 4, 1, 27646, 3},
				 8,
				 1,
				 1
				 }, */
				{
						"pgsqlPgAmopTable",
						"SELECT oid, amopfamily, amoplefttype, amoprighttype , amopstrategy , amopreqcheck , amopopr, amopmethod FROM pg_amop LIMIT 4",
						{ 1, 3, 6, 1, 4, 1, 27645, 1, 2, 3 }, 10, 1, 1 } };
/* TODO: End of replaceable stuff */

typedef struct cust_query_row
{
	netsnmp_index row_index;
	oid *myoids;
	pgsnmpd_query *query;
	int rownum;
} cust_query_row;

pgsnmpd_query *head;

pgsnmpd_query *read_custom_queries(void);
void free_query(pgsnmpd_query *query);
void fill_query_column_types(pgsnmpd_query *query);
void fill_query_container(pgsnmpd_query *query);

int custom_query_get_value(netsnmp_request_info *request, netsnmp_index *item,
		netsnmp_table_request_info *table_info);

int run_query(pgsnmpd_query *query);
int set_val_from_string(netsnmp_variable_list *var, u_char type, char *val);

/* 
 * Frees memory associated with an allocated (or partially allocated)
 * pgsnmpd_query structure, and all such structures following it in the 
 * query list
 */
void free_query(pgsnmpd_query *query)
{
	if (query == NULL)
		return;
	if (query->table_name != NULL)
		free(query->table_name);
	if (query->query_text != NULL)
		free(query->query_text);
	if (query->table_oid != NULL)
		free(query->table_oid);
	if (query->types != NULL)
		free(query->types);
	if (query->result != NULL)
		PQclear(query->result);
	if (query->next != NULL)
		free_query(query->next);
	free(query);
}

/* Allocates a pgsnmpd_query struct */
pgsnmpd_query *
alloc_custom_query(char *table_name, char *query_text, oid *table_oid,
		int oid_len)
{
	pgsnmpd_query *query;
	int i;

	query = malloc(sizeof(pgsnmpd_query));
	if (query == NULL)
		return NULL;
	query->result = NULL;
	query->next = NULL;
	query->last_refresh = 0;
	query->cache_timeout = -1;
	query->my_handler = NULL;

	i = strlen(table_name);
	if (i > CQ_MAX_LEN)
		i = CQ_MAX_LEN;
	query->table_name = malloc(i + 1);
	query->table_name[i] = '\0';
	strncpy(query->table_name, table_name, i);

	if (query->table_name == NULL)
	{
		free_query(query);
		return NULL;
	}

	i = strlen(query_text);
	if (i > CQ_MAX_LEN)
		i = CQ_MAX_LEN;
	query->query_text = malloc(i + 1);
	query->query_text[i] = '\0';
	strncpy(query->query_text, query_text, i);

	if (query->query_text == NULL)
	{
		free_query(query);
		return NULL;
	}

	query->table_oid = malloc(sizeof(oid) * oid_len);
	if (query->table_oid == NULL)
	{
		free_query(query);
		return NULL;
	}
	for (i = 0; i < oid_len; i++)
	{
		query->table_oid[i] = table_oid[i];
	}
	query->oid_len = oid_len;

	return query;
}

int run_query(pgsnmpd_query *query)
{
	/* TODO: remember to destroy all old rows when updating query. */
	time_t curtime;

	if (query == NULL)
		return -1;
	if (PQstatus(dbconn) != CONNECTION_OK)
		return -1;
	if (query->result != NULL)
		PQclear(query->result);

	query->result = PQexec(dbconn, query->query_text);
	if (PQresultStatus(query->result) != PGRES_TUPLES_OK)
	{
		snmp_log(LOG_ERR, "Failed to run query \"%s\"\n", query->query_text);
		PQclear(query->result);
		return -1;
	}
	curtime = time(NULL);
	query->last_refresh = curtime;
	query->colcount = PQnfields(query->result);
	query->rowcount = PQntuples(query->result);

	return 1;
}

void fill_query_column_types(pgsnmpd_query *query)
{
	int i;
	Oid type; /* NB! PostgreSQL's Oid, not Net-SNMP's oid */
	PGresult *res;
	const char *values[1];
	char param[10];

	/* This translates SQL types to SNMP types, as follows:
	 * Conversions for these four types are obvious
	 * ASN_INTEGER
	 * ASN_FLOAT
	 * ASN_BOOLEAN
	 *
	 * Everything else becomes a string:
	 * ASN_OCTET_STR
	 *
	 * Perhaps one day we'll also use ASN_DOUBLE
	 */

	if (query->result == NULL)
		return;

	values[0] = param;

	for (i = 0; i < query->colcount; i++)
	{
		type = PQftype(query->result, i);
		/*
		 * TODO: query pg_type table (including pg_type.h to use builtin
		 * constants got all kinds of errors I'd rather not deal with
		 */
		sprintf(param, "%d", type);
		res = PQexecPrepared(dbconn, "TYPEQUERY", 1, values, NULL, NULL, 0);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			snmp_log(LOG_ERR, "Couldn't determine column type\n");
		else
		{
			switch (atoi(PQgetvalue(res, 0, 0)))
			{
			case 0:
				query->types[i] = ASN_INTEGER;
				break;
			case 1:
				query->types[i] = ASN_FLOAT;
				break;
			case 2:
				query->types[i] = ASN_BOOLEAN;
				break;
			case 3:
				query->types[i] = ASN_OCTET_STR;
				break;
			default: /* If we get here, it's because the TYPEQUERY is b0rken */
				snmp_log(LOG_ERR,
						"Unknown column type translation. This is a bug.\n");
			}
		}
		PQclear(res);
	}
}

/* Read custom queries from configuration file, and populate a pgsnmpd_query list */
pgsnmpd_query *
read_custom_queries(void)
{
	/* TODO: Make this not fake */
	pgsnmpd_query *query, *last_query = NULL;
	int i;

	last_query = NULL;
	for (i = 0; i < q_len; i++)
	{
		/*
		 * Each query has two indexes in addition to those it declares:
		 * rdbmsDbIndex and pgsnmpdConnID. The latter tells us which connection
		 * configured into pgsnmpd we're talking about, for the day when we
		 * allow multiples. The former comes from RDBMS-MIB, and lets the user
		 * know we're talking about this PostgreSQL instance. Both are integers
		 */
		query = alloc_custom_query(q[i].table_name, q[i].query_string,
				q[i].query_oid, q[i].oid_len);
		if (query == NULL)
		{
			snmp_log(LOG_ERR, "Error allocating custom query structs");
			return NULL;
		}

		/* Run query, figure out types of index columns, and save the PQresult */
		run_query(query);
		query->num_indexes = q[i].index_cols;
		query->min_colnum = q[i].min_colnum;
		query->types = malloc(sizeof(oid) * query->colcount);
		if (query->types == NULL)
		{
			snmp_log(LOG_ERR, "Memory allocation problem");
			return NULL;
		}
		fill_query_column_types(query);

		query->next = last_query;
		last_query = query;
	}

	return last_query;
}

/* General initialization */
void init_custom_queries(void)
{
	netsnmp_table_registration_info *table_info;
	pgsnmpd_query *curquery;
	int i;
	PGresult *res;

	if (custom_query_config_file == NULL)
		return;

	/*
	 ASN_INTEGER = 0
	 ASN_FLOAT = 1
	 ASN_BOOLEAN = 2
	 ASN_OCTET_STR = 3
	 */

	res = PQprepare(dbconn, "TYPEQUERY", "SELECT CASE "
		"WHEN typname LIKE 'int%' OR typname = 'xid' OR typname = 'oid'"
		"THEN 0 "
		"WHEN typname LIKE 'float%' THEN 1 "
		"WHEN typname = 'bool' THEN 2 "
		"ELSE 3 "
		"END "
		"FROM pg_catalog.pg_type WHERE oid = $1", 1, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		snmp_log(LOG_ERR, "Failed to prepare statement (error: %s)\n",
				PQresStatus(PQresultStatus(res)));
		return;
	}
	PQclear(res);

	head = parse_config(custom_query_config_file);
	if (head == NULL)
	{
		snmp_log(LOG_INFO, "No custom queries initialized\n");
		return;
	}

	for (curquery = head; curquery != NULL; curquery = curquery->next)
	{
		run_query(curquery);
		curquery->types = malloc(sizeof(oid) * curquery->colcount);
		if (curquery->types == NULL)
		{
			snmp_log(LOG_ERR, "Memory allocation problem");
			return;
		}
		fill_query_column_types(curquery);
		if (curquery->my_handler)
		{
			snmp_log(LOG_ERR,
					"init_custom_queries called again for query %s\n",
					curquery->table_name);
			return;
		}

		memset(&(curquery->cb), 0x00, sizeof(curquery->cb));

		/** create the table structure itself */
		table_info = SNMP_MALLOC_TYPEDEF(netsnmp_table_registration_info);
		curquery->my_handler = netsnmp_create_handler_registration(
				curquery->table_name, netsnmp_table_array_helper_handler,
				curquery->table_oid, curquery->oid_len, HANDLER_CAN_RONLY);

		if (!curquery->my_handler || !table_info)
		{
			snmp_log(LOG_ERR, "malloc failed in init_custom_queries\n");
			return; /** mallocs failed */
		}

		netsnmp_table_helper_add_index(table_info, ASN_INTEGER); /* pgsnmpdConnID */
		netsnmp_table_helper_add_index(table_info, ASN_INTEGER); /* rdbmsDbIndex */
		for (i = 0; i < curquery->num_indexes; i++)
			netsnmp_table_helper_add_index(table_info, curquery->types[i]);

		table_info->min_column = curquery->min_colnum;
		table_info->max_column = curquery->colcount;

		curquery->cb.get_value = custom_query_get_value;
		curquery->cb.container = netsnmp_container_find("table_container");

		DEBUGMSGTL(("init_custom_queries",
						"Registering table for query "
						"as a table array\n"));
		switch (netsnmp_table_container_register(curquery->my_handler,
				table_info, &curquery->cb, curquery->cb.container, 1))
		{
		case MIB_REGISTRATION_FAILED:
			snmp_log(LOG_INFO, "Failed to register table %s\n",
					curquery->table_name);
			break;
		case MIB_DUPLICATE_REGISTRATION:
			snmp_log(LOG_INFO, "Duplicate registration for table %s\n",
					curquery->table_name);
			break;
		case MIB_REGISTERED_OK:
			DEBUGMSGTL(("init_custom_queries",
							"Successfully registered table %s\n", curquery->table_name));
			break;
		default:
			snmp_log(LOG_INFO, "Unknown registration result for table %s\n",
					curquery->table_name);
		}

		/* Having set everything up, fill the table's container with data */
		fill_query_container(curquery);
	}
}

void fill_query_container(pgsnmpd_query *query)
{
	/* TODO: Make this work */
	cust_query_row *row;
	int i, j, string_idx_count, string_idx_len;
	netsnmp_variable_list var_rdbmsDbIndex, var_pgsnmpdConnID,
			*var_otherIndexes;
	int err = SNMP_ERR_NOERROR;
	char *val;

	memset(&var_pgsnmpdConnID, 0x00, sizeof(var_pgsnmpdConnID));
	var_pgsnmpdConnID.type = ASN_INTEGER;
	memset(&var_rdbmsDbIndex, 0x00, sizeof(var_rdbmsDbIndex));
	var_rdbmsDbIndex.type = ASN_INTEGER;

	var_otherIndexes = malloc(sizeof(netsnmp_variable_list)
			* query->num_indexes);
	if (var_otherIndexes == NULL)
	{
		snmp_log(LOG_ERR, "Memory allocation error\n");
		return;
	}
	memset(var_otherIndexes, 0, sizeof(netsnmp_variable_list)
			* query->num_indexes);

	string_idx_count = 0;
	for (i = 0; i < query->num_indexes; i++)
	{
		if (i < query->num_indexes - 1)
			var_otherIndexes[i].next_variable = &var_otherIndexes[i + 1];
		var_otherIndexes[i].type = query->types[i];
		if (query->types[i] == ASN_OCTET_STR)
			string_idx_count++;
	}

	var_pgsnmpdConnID.next_variable = &var_rdbmsDbIndex;
	var_rdbmsDbIndex.next_variable = var_otherIndexes;

	snmp_set_var_typed_value(&var_rdbmsDbIndex, ASN_INTEGER,
			(u_char *) &rdbmsDbIndex, sizeof(int));
	snmp_set_var_typed_value(&var_pgsnmpdConnID, ASN_INTEGER,
			(u_char *) &pgsnmpdConnID, sizeof(int));

	for (i = 0; i < query->rowcount; i++)
	{
		string_idx_len = 0;
		for (j = 0; j < query->num_indexes; j++)
		{
			val = PQgetvalue(query->result, i, j);
			/* TODO: Floats also need more than the usual memory. Learn to handle them */
			if (query->types[j] == ASN_OCTET_STR)
				string_idx_len += strlen(val);
			set_val_from_string(&var_otherIndexes[j], query->types[j], val);
		}
		row = SNMP_MALLOC_TYPEDEF(cust_query_row);
		row->myoids = malloc(sizeof(oid) * (query->num_indexes
				+ extra_idx_count + string_idx_count + string_idx_len));
		if (row->myoids == NULL)
		{
			snmp_log(LOG_ERR, "memory allocation problem \n");
			return;
		}

		row->row_index.len = query->num_indexes + extra_idx_count
				+ string_idx_len + string_idx_count;
		row->row_index.oids = row->myoids;

		err = build_oid_noalloc(row->row_index.oids, row->row_index.len,
				(size_t *) &(row->row_index.len), NULL, 0, &var_pgsnmpdConnID);
		if (err)
			snmp_log(LOG_ERR, "error %d converting index to oid, query %s\n",
					err, query->table_name);

		row->query = query;
		row->rownum = i;

		CONTAINER_INSERT(query->cb.container, row);
	}
}

/*
 * custom_query_get_value
 *
 * This routine is called for get requests to copy the data
 * from the context to the varbind for the request. If the
 * context has been properly maintained, you don't need to
 * change in code in this fuction.
 */
int custom_query_get_value(netsnmp_request_info *request, netsnmp_index *item,
		netsnmp_table_request_info *table_info)
{
	int column = table_info->colnum;
	netsnmp_variable_list *var = request->requestvb;
	cust_query_row *context = (cust_query_row *) item;

	if (context->query->result == NULL)
	{
		snmp_log(LOG_ERR, "No valid result for table\n");
		/* TODO: Make this less fatal? */
		return SNMP_ERR_GENERR;
	}
	if (column > context->query->colcount + extra_idx_count)
	{
		snmp_log(LOG_ERR, "Unknown column in requested table\n");
		return SNMP_ERR_GENERR;
	}

	return set_val_from_string(var, context->query->types[column
			- context->query->min_colnum], PQgetvalue(context->query->result,
			context->rownum, column - context->query->min_colnum));
}

int set_val_from_string(netsnmp_variable_list *var, u_char type, char *val)
{
	int i;
	float f;

	switch (type)
	{
	case ASN_INTEGER:
		i = atoi(val);
		snmp_set_var_typed_value(var, ASN_INTEGER, (u_char *) &i, sizeof(int));
		break;
	case ASN_FLOAT:
		f = strtof(val, NULL);
		snmp_set_var_typed_value(var, ASN_OPAQUE_FLOAT, (u_char *) &f,
				sizeof(float));
		break;
	case ASN_BOOLEAN:
		if (val[0] == 't')
			i = 1;
		else
			i = 2;
		snmp_set_var_typed_value(var, ASN_INTEGER, (u_char *) &i, sizeof(int));
		break;
	case ASN_OCTET_STR:
		snmp_set_var_typed_value(var, type, (u_char *) val, strlen(val)
				* sizeof(char));
		break;
	default:
		snmp_log(LOG_ERR, "Unknown data type. This is a bug.\n");
		return SNMP_ERR_GENERR;
	}
	return SNMP_ERR_NOERROR;
}
