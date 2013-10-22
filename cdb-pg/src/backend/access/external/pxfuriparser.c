
#include "access/pxfuriparser.h"
#include "catalog/pg_exttable.h"
#include "utils/formatting.h"
#include "utils/uri.h"

static const char* segwork_substring = "segwork=";
static const char segwork_separator = '@';
static const int EMPTY_VALUE_LEN = 2;

static void  GPHDUri_parse_protocol(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_authority(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_data(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_options(GPHDUri *uri, char **cursor);
static List* GPHDUri_parse_option(char* pair, List* options, const char* uri);
static void  GPHDUri_free_options(GPHDUri *uri);
static void  GPHDUri_parse_segwork(GPHDUri *uri, const char *uri_str);
static List* GPHDUri_parse_fragment(char* fragment, List* fragments);
static void  GPHDUri_free_fragments(GPHDUri *uri);
static char* GPHDUri_dup_without_segwork(const char* uri);
static void  GPHDUri_debug_print_options(GPHDUri *uri);
static void  GPHDUri_debug_print_segwork(GPHDUri *uri);

/* parseGPHDUri
 *
 * Go over a URI string and parse it into its various components while
 * verifying valid structure given a specific target protocol.
 *
 * URI format:
 * 		<protocol name>://<authority>/<data>?<option>&<option>&<...>&segwork=<segwork>
 *
 *
 * protocol name	- must be 'pxf'
 * authority		- host:port
 * data				- data path (directory name/table name/etc., depending on target)
 * options			- valid options are dependent on the protocol. Each
 * 					  option is a key value pair.
 * 					  segwork option is not a user option but a gp master
 * 					  option. It is removed as fast as possible from the uri
 * 					  so errors, won't be printed with it to the user.
 *
 * inputs:
 * 		'uri_str'	- the raw uri str
 *
 * returns:
 * 		a parsed uri as a GPHDUri structure, or reports a format error.
 */
GPHDUri*
parseGPHDUri(const char *uri_str)
{
	GPHDUri	*uri = (GPHDUri *)palloc0(sizeof(GPHDUri));
	char	*cursor;

	uri->uri = GPHDUri_dup_without_segwork(uri_str);
	cursor = uri->uri;

	GPHDUri_parse_segwork(uri, uri_str);
	GPHDUri_parse_protocol(uri, &cursor);
	GPHDUri_parse_authority(uri, &cursor);
	GPHDUri_parse_data(uri, &cursor);
	GPHDUri_parse_options(uri, &cursor);

	return uri;
}

void
freeGPHDUri(GPHDUri *uri)
{

	pfree(uri->protocol);
	GPHDUri_free_fragments(uri);

	pfree(uri->host);
	pfree(uri->port);
	pfree(uri->data);

	GPHDUri_free_options(uri);

	pfree(uri);
}

/*
 * GPHDUri_get_value_for_opt
 *
 * Given a key, find the matching val and assign it to 'val'.
 * If 'emit_error' is set, report an error and quit if the
 * requested key or its value is missing.
 *
 * Returns 0 if the key was found, -1 otherwise.
 */
int
GPHDUri_get_value_for_opt(GPHDUri *uri, char *key, char **val, bool emit_error)
{
	ListCell	*item;

	foreach(item, uri->options)
	{
		OptionData *data = (OptionData*)lfirst(item);

		if (pg_strcasecmp(data->key, key) == 0)
		{
			*val = data->value;

			if (emit_error && !(*val))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("No value assigned to the %s option in "
								"%s", key, uri->uri)));

			return 0;
		}
	}

	if (emit_error)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Missing %s option in %s", key, uri->uri)));

	return -1;
}

/*
 * GPHDUri_verify_no_duplicate_options
 * verify each option appears only once (case insensitive)
 */
void
GPHDUri_verify_no_duplicate_options(GPHDUri *uri)
{
	ListCell *option = NULL;
	List *duplicateKeys = NIL;
	List *previousKeys = NIL;
	StringInfoData duplicates;
	initStringInfo(&duplicates);

	foreach(option, uri->options)
	{
		OptionData *data = (OptionData*)lfirst(option);
		Value *key = makeString(str_toupper(data->key, strlen(data->key)));

		if(!list_member(previousKeys, key))
		{
			previousKeys = lappend(previousKeys, key);
		}	
		else if(!list_member(duplicateKeys, key))
		{
			duplicateKeys = lappend(duplicateKeys, key);
			appendStringInfo(&duplicates, "%s, ", strVal(key));
		}
	}

	if(duplicates.len > 0)
	{
		truncateStringInfo(&duplicates, duplicates.len - strlen(", ")); //omit trailing ', ' 
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Invalid URI %s: Duplicate option(s): %s", uri->uri, duplicates.data)));
	}
	list_free(duplicateKeys);
	list_free(previousKeys);
	pfree(duplicates.data);
}

/*
 * GPHDUri_verify_core_options_exist
 * This function is given a list of core options to verify their existence.
 */
void
GPHDUri_verify_core_options_exist(GPHDUri *uri, List *coreOptions)
{
	char *key = NULL;
	ListCell *coreOption = NULL;	
	StringInfoData missing;
	initStringInfo(&missing);
	
	foreach(coreOption, coreOptions)
	{
		bool optExist = false;
		ListCell *option = NULL;
		foreach(option, uri->options)
		{
			key = ((OptionData*)lfirst(option))->key;
			if (pg_strcasecmp(key, lfirst(coreOption)) == 0)
			{
				optExist = true;
				break;
			}
		}
		if(!optExist)
		{
			appendStringInfo(&missing, "%s and ", (char*)lfirst(coreOption));
		}
	}

	if(missing.len > 0)
	{
		truncateStringInfo(&missing, missing.len - strlen(" and ")); //omit trailing ' and ' 
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Invalid URI %s: PROFILE or %s option(s) missing", uri->uri, missing.data)));
	}
	pfree(missing.data);		
}

/*
 * GPHDUri_parse_protocol
 *
 * Parse the protocol section of the URI which is passed down
 * in 'cursor', having 'cursor' point at the current string
 * location. Set the protocol string and the URI type.
 *
 * See parseGPHDUri header for URI structure description.
 */
static void
GPHDUri_parse_protocol(GPHDUri *uri, char **cursor)
{
	const char *ptc_sep = "://";
	int			ptc_sep_len = strlen(ptc_sep);
	char 	   *post_ptc;
	char	   *start = *cursor;
	int			ptc_len;

	post_ptc = strstr(start, ptc_sep);

	if(!post_ptc)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s", uri->uri)));

	ptc_len = post_ptc - start;
	uri->protocol = pnstrdup(start, ptc_len);

	if (!IS_PXF_URI(uri->uri))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s : unsupported protocol '%s'",
						uri->uri, uri->protocol)));

	/* set cursor to new position and return */
	*cursor = start + ptc_len + ptc_sep_len;
}

/*
 * GPHDUri_parse_authority
 *
 * Parse the authority section of the URI which is passed down
 * in 'cursor', having 'cursor' point at the current string
 * location.
 *
 * See parseGPHDUri header for URI structure description.
 */
static void
GPHDUri_parse_authority(GPHDUri *uri, char **cursor)
{
	char		*start = *cursor;
	char		*end;
	const char	*default_port = "9000"; /* TODO: port is no longer used, so no need for that */


	if (*start == '/')
	{
		/* implicit authority 'localhost:defport' (<ptc>:///) */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s : missing authority section",
						 uri->uri)));
	}
	else
	{
		end = strchr(start, '/');

		if (!end)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Invalid URI %s : missing authority section",
							 uri->uri)));
		}
		else
		{
			char	*colon;
			char	*p;
			int		 len = end - start;

			/* host */
			uri->host = pnstrdup(start, len);

			/* find the port ':' */
			p = strchr(uri->host, ']');
			if (p)
			{
				/* IPV6 */
				colon = strchr(p, ':');

				/* TODO: may need to remove brackets from host. Ask Alex */
			}
			else
			{
				/* IPV4 */
				colon = strchr(uri->host, ':');
			}

			/* port */
			if (colon)
			{
				uri->port = pstrdup(colon + 1);

				/* now truncate ":<port>" from hostname */
				uri->host[len - strlen(colon)] = '\0';

				*colon = 0;
			}
			else
			{
				/* default port value */

				uri->port = pstrdup(default_port);
			}
		}
	}

	/* skip the authority trailing slash */
	*cursor = ++end;
}

/*
 * GPHDUri_parse_data
 *
 * Parse the data section of the URI which is passed down
 * in 'cursor', having 'cursor' point at the current string
 * location.
 *
 * See parseGPHDUri header for URI structure description.
 */
static void
GPHDUri_parse_data(GPHDUri *uri, char **cursor)
{
	char	*start = *cursor;
	char	*options_section = strrchr(start, '?');
	size_t	 data_len;

	/*
	 * If there exists an 'options' section, the data section length
	 * is from start point to options section point. Otherwise, the
	 * data section length is the remaining string length from start.
	 */
	if (options_section)
		data_len = options_section - start;
	else
		data_len = strlen(start);

	uri->data = pnstrdup(start, data_len);
	*cursor += data_len;
}

/*
 * GPHDUri_parse_options
 *
 * Parse the data section of the URI which is passed down
 * in 'cursor', having 'cursor' point at the current string
 * location.
 *
 * See parseGPHDUri header for URI structure description.
 */
static void
GPHDUri_parse_options(GPHDUri *uri, char **cursor)
{
	char    *dup = pstrdup(*cursor);
	char	*start = dup;

	/* option section must start with '?'. if absent, there are no options */
	if (!start || start[0] != '?')
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: missing options section",
						uri->uri)));

	/* skip '?' */
	start++;

	/* sanity check */
	if (strlen(start) < 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: invalid option after '?'",
						uri->uri)));

	/* ok, parse the options now */

	const char	*sep = "&";
	char		*strtok_context;
	char		*pair;

	for (pair = strtok_r(start, sep, &strtok_context);
			pair;
			pair = strtok_r(NULL, sep, &strtok_context))
	{
		uri->options = GPHDUri_parse_option(pair, uri->options, uri->uri);
	}

	pfree(dup);
}

/*
 * Parse an option in the form:
 * <key>=<value>
 * to OptionData object (key and value).
 */
static List*
GPHDUri_parse_option(char* pair, List* options, const char* uri)
{

	char	*sep;
	int		pair_len, key_len, value_len;
	OptionData* option_data;

	option_data = palloc0(sizeof(OptionData));
	pair_len = strlen(pair);

	sep = strchr(pair, '=');
	if (sep == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: option '%s' missing '='", uri, pair)));
	}

	if (strchr(sep + 1, '=') != NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: option '%s' contains duplicate '='", uri, pair)));
	}

	key_len = sep - pair;
	if (key_len == 0) {
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: option '%s' missing key before '='", uri, pair)));
	}
	
	value_len = pair_len - key_len + 1;
	if (value_len == EMPTY_VALUE_LEN) {
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: option '%s' missing value after '='", uri, pair)));
	}
    
	option_data->key = pnstrdup(pair,key_len);
	option_data->value = pnstrdup(sep + 1, value_len);

	return lappend(options, option_data);
}

/*
 * Free options list
 */
static void
GPHDUri_free_options(GPHDUri *uri)
{
	ListCell *option = NULL;

	foreach(option, uri->options)
	{
		OptionData *data = (OptionData*)lfirst(option);
		pfree(data->key);
		pfree(data->value);
		pfree(data);
	}
	list_free(uri->options);
	uri->options = NIL;
}

/*
 * GPHDUri_debug_print
 *
 * For debugging while development only.
 */
void
GPHDUri_debug_print(GPHDUri *uri)
{

	elog(NOTICE,
		 "URI: %s, Host: %s, Port: %s, Data Path: %s",
		 uri->uri,
		 uri->host,
		 uri->port,
		 uri->data);

	GPHDUri_debug_print_options(uri);
	GPHDUri_debug_print_segwork(uri);
}

/*
 * GPHDUri_debug_print_options
 *
 * For debugging while development only.
 */
void
GPHDUri_debug_print_options(GPHDUri *uri)
{
	ListCell	*item;
	int			count = 0;

	elog(NOTICE, "options section data: ");
	foreach(item, uri->options)
	{
		OptionData *data = (OptionData*)lfirst(item);
		elog(NOTICE,
			 "%u: key: %s, value: %s",
			 count, data->key, data->value);
		++count;
	}
}

/*
 * GPHDUri_debug_print_segwork
 *
 * For debugging while development only.
 */
void
GPHDUri_debug_print_segwork(GPHDUri *uri)
{
	ListCell	*item;
	int			count = 0;

	elog(NOTICE, "segwork section data: ");
	foreach(item, uri->fragments)
	{
		FragmentData *data = (FragmentData*)lfirst(item);
		if (data->user_data)
		{
			elog(NOTICE,
				 "%u: authority: %s, index %s, user data: %s",
				 count, data->authority, data->index, data->user_data);
		}
		else
		{
			elog(NOTICE,
				 "%u: authority: %s, index %s",
				 count, data->authority, data->index);
		}
		++count;
	}
}

/*
 * GPHDUri_parse_segwork parses the segwork section of the uri.
 * ...&segwork=<size>@<ip>@<port>@<index><size>@<ip>@<port>@<index><size>...
 */
static void
GPHDUri_parse_segwork(GPHDUri *uri, const char *uri_str)
{
	char		*segwork;
	char		*fragment;
	char		*size_end;
	int 		 fragment_size, count = 0;

	/* skip segwork= */
	segwork = strstr(uri_str, segwork_substring);
	if (segwork == NULL)
		return;

	segwork += strlen(segwork_substring);

	/*
	 * read next segment.
	 * each segment is prefixed its size.
	 */
	while (segwork && strlen(segwork))
	{
		/* expect size */
		size_end = strchr(segwork, segwork_separator);
		Assert(size_end != NULL);
		*size_end = '\0';
		fragment_size = atoi(segwork);
		segwork = size_end + 1; /* skip the size field */
		Assert(fragment_size <= strlen(segwork));

		fragment = pnstrdup(segwork, fragment_size);
		elog(DEBUG2, "GPHDUri_parse_segwork: fragment #%d, size %d, str %s", count, fragment_size, fragment);
		uri->fragments = GPHDUri_parse_fragment(fragment, uri->fragments);
		segwork += fragment_size;
		++count;
		pfree(fragment);
	}

}

/*
 * Parsed a fragment string in the form:
 * <ip>@<port>@<index>[@user_data] - 192.168.1.1@1422@1[@user_data]
 * to authority ip:port - 192.168.1.1:1422
 * to index - 1
 * user data is optional
 */
static List*
GPHDUri_parse_fragment(char* fragment, List* fragments)
{

	char	*dup_frag = pstrdup(fragment);
	char	*value_start;
	char	*value_end;
	bool	has_user_data = false;

	StringInfoData formatter;
	FragmentData* fragment_data;

	fragment_data = palloc0(sizeof(FragmentData));
	initStringInfo(&formatter);

	value_start = dup_frag;
	/* expect ip */
	value_end = strchr(value_start, segwork_separator);
	Assert(value_end != NULL);
	*value_end = '\0';
	appendStringInfo(&formatter, "%s:", value_start);
	value_start = value_end + 1;
	/* expect port */
	value_end = strchr(value_start, segwork_separator);
	Assert(value_end != NULL);
	*value_end = '\0';
	appendStringInfo(&formatter, "%s", value_start);
	fragment_data->authority = formatter.data;
	value_start = value_end + 1;
	/* expect source name */
	value_end = strchr(value_start, segwork_separator);
	Assert(value_end != NULL);
	*value_end = '\0';
	fragment_data->source_name = pstrdup(value_start);
	value_start = value_end + 1;
	/* expect index */
	Assert(value_start);

	/* check for user data */
	value_end = strchr(value_start, segwork_separator);
	if (value_end != NULL)
	{
		has_user_data = true;
		*value_end = '\0';
	}
	fragment_data->index = pstrdup(value_start);

	/* read user data */
	if (has_user_data)
	{
		fragment_data->user_data = pstrdup(value_end + 1);
	}

	return lappend(fragments, fragment_data);
}

/*
 * Free fragments list
 */
static void 
GPHDUri_free_fragments(GPHDUri *uri)
{
	ListCell *fragment = NULL;
	
	foreach(fragment, uri->fragments)
	{
		FragmentData *data = (FragmentData*)lfirst(fragment);
		pfree(data->authority);
		pfree(data->index);
		pfree(data->source_name);
		pfree(data);
	}
	list_free(uri->fragments);
	uri->fragments = NIL;
}

/*
 * Returns a uri without the segwork section.
 * segwork section removed so users won't get it 
 * when an error occurs and the uri is printed
 */
static char* 
GPHDUri_dup_without_segwork(const char* uri)
{
	char	*segwork;
	char	*no_segwork;

	no_segwork = pstrdup(uri);
	segwork = strstr(no_segwork, segwork_substring);

	/* If segwork_substring was not found,
	 * just return a dup of the string
	 */
	if (segwork != NULL)
	{
		/* back 1 char to include either & or ? */
		--segwork;
		*segwork = 0;
	}

	return no_segwork;
}

/* --------------------------------
 *		RelationIsExternalPxf -
 *
 *		Check if a table is an external PXF tbl.
 * --------------------------------
 */
bool RelationIsExternalPxf(Relation rel, StringInfo location)
{
	ExtTableEntry	*tbl;
	List			*locsList;
	ListCell		*cell;

	if (!RelationIsExternal(rel))
		return false;

	tbl = GetExtTableEntry(rel->rd_id);
	Assert(tbl);

	locsList = tbl->locations;

	foreach(cell, locsList)
	{
		char* locsItem = strVal(lfirst(cell));

		if (!locsItem)
			continue;

		if (IS_PXF_URI(locsItem))
		{
			appendStringInfoString(location, locsItem);
			pfree(tbl);
			return true;
		}
	}
	pfree(tbl);

	return false;
}

