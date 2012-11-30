#include <yaml.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include "query_reader.h"

/* TODO: Return value checking of strdup, etc. is fairly haphazard. */

void print_error(yaml_parser_t *parser);

typedef enum query_reader_state_s
{
	INIT,
	READ_QUERY,
	NAME_VALUE,
	QUERY_VALUE,
	OID_VALUE,
	INDEXES_VALUE,
    MINCOL_VALUE,
    TYPES_VALUE,
    TYPE_MAPPING_VALUE
} query_reader_state_t;

oid *parse_oid(const char *oid_str_orig, int *oid_len);

oid *
parse_oid(const char *oid_str_orig, int *oid_len)
{
	int len = strlen(oid_str_orig), i;
	char *a, *b, *oid_str;
	oid *result;

	oid_str = strdup((char *) oid_str_orig);
	if (oid_str == NULL)
	{
		snmp_log(LOG_ERR, "Couldn't allocate copy of OID string\n");
		exit(1);
	}
	a = oid_str;

	result = malloc(len * sizeof(oid));
	if (result == NULL)
	{
		snmp_log(LOG_ERR, "Couldn't allocate array of %d OIDs\n", *oid_len);
		exit(1);
	}
	i = 0;

	a = oid_str;
	*oid_len = 0;
	while (a - oid_str < len)
	{
		b = strpbrk(a, ".");
		if (b == NULL)
		{
			result[i] = atoi(a);
			(*oid_len)++;
			break;
		}
		else
		{
			*b = '\0';
			result[i++] = atoi(a);
			a = b + 1;
			(*oid_len)++;
		}
	}
	free(oid_str);
	return result;
}

pgsnmpd_query *
parse_config(const char *filename)
{
	FILE *inputfile;
	yaml_parser_t parser;
	yaml_event_t event;
	int done = 0;
	int error = 0;
	pgsnmpd_query *head = NULL, *cur = NULL, *newquery = NULL;
	query_reader_state_t st = INIT;
	char *name_str = NULL, *query_str = NULL, *oid_str = NULL;
	int indexes_val = 0, mincol_val = 0, query_count = 0;
	oid *oid_arr = NULL;
	int oid_len = 1;

	yaml_parser_initialize(&parser);
	inputfile = fopen(filename, "rb");
	if (inputfile == NULL)
	{
		snmp_log(LOG_ERR, "Problem opening input file \"%s\": %s\n", filename,
				strerror(errno));
		return 0;
	}
	yaml_parser_set_input_file(&parser, inputfile);

	while (!done)
	{
		if (!yaml_parser_parse(&parser, &event))
		{
			error = 1;
			break;
		}
		done = (event.type == YAML_STREAM_END_EVENT);
		/* head = process_event(&event); */
		switch (st)
		{
		case INIT:
			if (event.type == YAML_SCALAR_EVENT && strncmp(
					(char *) event.data.scalar.value, "queries", 8) == 0)
			{
				st = READ_QUERY;
				if (name_str != NULL)
					free(name_str);
				name_str = NULL;
				if (query_str != NULL)
					free(query_str);
				query_str = NULL;
				if (oid_str != NULL)
					free(oid_str);
				oid_str = NULL;
				indexes_val = -1;
				mincol_val = -1;
			}
			query_count++;
			break;
		case READ_QUERY:
			if (event.type == YAML_SCALAR_EVENT)
			{
				if (strncmp((char *) event.data.scalar.value, "name", 5) == 0)
					st = NAME_VALUE;
				else if (strncmp((char *) event.data.scalar.value, "query", 6)
						== 0)
					st = QUERY_VALUE;
				else if (strncmp((char *) event.data.scalar.value, "oid", 6)
						== 0)
					st = OID_VALUE;
				else if (strncmp((char *) event.data.scalar.value, "indexes", 9)
						== 0)
					st = INDEXES_VALUE;
				else if (strncmp((char *) event.data.scalar.value,
						"min_column", 15) == 0)
					st = MINCOL_VALUE;
				else
				{
					snmp_log(LOG_ERR,
							"Unexpected data object reading query %d\n",
							query_count);
					return NULL;
				}
			}
			else if (event.type == YAML_MAPPING_END_EVENT)
			{
				if (indexes_val == -1 || mincol_val == -1 || name_str == NULL
						|| oid_str == NULL || query_str == NULL)
				{
					snmp_log(LOG_ERR,
							"Error reading query %d (%s): %s%s%s%s%s\n",
							query_count, name_str == NULL ? "Name Unknown"
									: name_str,
							name_str == NULL ? "Name not specified" : "",
							oid_str == NULL ? "OID not specified" : "",
							query_str == NULL ? "Query not specified" : "",
							indexes_val == -1 ? "Indexes not specified" : "",
							mincol_val == -1 ? "Min column not specified" : "");
					return NULL;
				}

				oid_arr = parse_oid(oid_str, &oid_len);

				newquery = alloc_custom_query(name_str, query_str, oid_arr,
						oid_len);
				newquery->num_indexes = indexes_val;
				newquery->min_colnum = mincol_val;
				if (head == NULL)
				{
					head = newquery;
					cur = head;
				}
				else
				{
					cur->next = newquery;
					cur = newquery;
				}
			}
			else if (event.type == YAML_SEQUENCE_END_EVENT)
				st = INIT;
			break;
		case NAME_VALUE:
			if (event.type == YAML_SCALAR_EVENT)
			{
				name_str = strdup((char *) event.data.scalar.value);
				st = READ_QUERY;
			}
			else
			{
				snmp_log(
						LOG_ERR,
						"Unexpected YAML event trying to read name of query %d\n",
						query_count);
				return NULL;
			}
			break;
		case QUERY_VALUE:
			if (event.type == YAML_SCALAR_EVENT)
			{
				query_str = strdup((char *) event.data.scalar.value);
				st = READ_QUERY;
			}
			else
			{
				snmp_log(
						LOG_ERR,
						"Unexpected YAML event trying to read query string of query %d\n",
						query_count);
				return NULL;
			}
			break;
		case OID_VALUE:
			if (event.type == YAML_SCALAR_EVENT)
			{
				oid_str = strdup((char *) event.data.scalar.value);
				st = READ_QUERY;
			}
			else
			{
				snmp_log(
						LOG_ERR,
						"Unexpected YAML event trying to read OID of query %d\n",
						query_count);
				return NULL;
			}
			break;
		case INDEXES_VALUE:
			if (event.type == YAML_SCALAR_EVENT)
			{
				indexes_val = atoi((char *) event.data.scalar.value);
				st = READ_QUERY;
			}
			else
			{
				snmp_log(
						LOG_ERR,
						"Unexpected YAML event trying to read indexes of query %d\n",
						query_count);
				return NULL;
			}
			break;
		case MINCOL_VALUE:
			if (event.type == YAML_SCALAR_EVENT)
			{
				mincol_val = atoi((char *) event.data.scalar.value);
				st = READ_QUERY;
			}
			else
			{
				snmp_log(
						LOG_ERR,
						"Unexpected YAML event trying to read min_column of query %d\n",
						query_count);
				return NULL;
			}
			break;
		default:
			printf("bug in case statement\n");
		}
		yaml_event_delete(&event);
	}

	if (error)
		print_error(&parser);

	yaml_parser_delete(&parser);
	fclose(inputfile);
	printf("deleted parser\n");

	/* Print out results
	 cur = head;
	 while (cur != NULL) {
	 printf("Query:\n\tname: %s\n\tquery: %s\n\toid len: %d\n\tindexes: %d\n\tmin_col: %d\n",
	 cur->table_name,
	 cur->query_text,
	 cur->oid_len,
	 cur->num_indexes,
	 cur->min_colnum);
	 printf("\tOID: ");
	 done = 0;
	 while (done < cur->oid_len)
	 printf(".%u", (u_int) cur->table_oid[done++]);
	 printf("\n");
	 cur = cur->next;
	 }
	 */
	return head;
}

void print_error(yaml_parser_t *parser)
{
	snmp_log(LOG_ERR,
			"YAML parse error: %s at offset %d, value %d, context \"%s\"\n",
			parser->problem, (int) parser->problem_offset,
			(int) parser->problem_value, parser->context);

	snmp_log(LOG_ERR, "Problem mark: index %d, line %d, column %d\n",
			(int) parser->problem_mark.index, (int) parser->problem_mark.line,
			(int) parser->problem_mark.column);
	snmp_log(LOG_ERR, "Context marK: index %d, line %d, column %d\n",
			(int) parser->context_mark.index, (int) parser->context_mark.line,
			(int) parser->context_mark.column);

	switch (parser->error)
	{
	case YAML_NO_ERROR:
		snmp_log(LOG_ERR, "\tYAML_NO_ERROR\n");
		break;
	case YAML_MEMORY_ERROR:
		snmp_log(LOG_ERR, "\tYAML_MEMORY_ERROR\n");
		break;
	case YAML_READER_ERROR:
		snmp_log(LOG_ERR, "\tYAML_READER_ERROR\n");
		break;
	case YAML_SCANNER_ERROR:
		snmp_log(LOG_ERR, "\tYAML_SCANNER_ERROR\n");
		break;
	case YAML_PARSER_ERROR:
		snmp_log(LOG_ERR, "\tYAML_PARSER_ERROR\n");
		break;
	case YAML_COMPOSER_ERROR:
		snmp_log(LOG_ERR, "\tYAML_COMPOSER_ERROR\n");
		break;
	case YAML_WRITER_ERROR:
		snmp_log(LOG_ERR, "\tYAML_WRITER_ERROR\n");
		break;
	case YAML_EMITTER_ERROR:
		snmp_log(LOG_ERR, "\tYAML_EMITTER_ERROR\n");
		break;
	}
}
