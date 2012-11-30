#include "uriparser.h"
#include "utils/formatting.h"

static const char* segwork_substring = "segwork=";

static void  GPHDUri_parse_protocol(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_authority(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_type(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_data(GPHDUri *uri, char **cursor);
static void  GPHDUri_parse_options(GPHDUri *uri, char **cursor);
static void  GPHDUri_verify_type(GPHDUri *uri);
static void  GPHDUri_parse_segwork(GPHDUri *uri, const char *uri_str);
static List* GPHDUri_parse_fragment(char* fragment, List* fragments);
static void  GPHDUri_free_fragments(GPHDUri *uri);
static char* GPHDUri_dup_without_segwork(const char* uri);
static void  GPHDUri_debug_print_segwork(GPHDUri *uri);

/* parseGPHDUri
 *
 * Go over a URI string and parse it into its various components while
 * verifying valid structure given a specific target protocol.
 *
 * URI format:
 * 		<protocol name>://<authority>/<type>/<data>?<option>&<option>&<...>&segwork=<segwork>
 *
 * currently supported protocols for HD are 'gphdfs' and 'gphbase'.
 *
 * protocol name	- must be 'gpfusion'
 * authority		- host:port
 * type				- either 'hdfs' or 'hbase'
 * data				- directory name (gphdfs) or table name (gphbase)
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
	GPHDUri_parse_type(uri, &cursor);
	GPHDUri_parse_data(uri, &cursor);
	GPHDUri_parse_options(uri, &cursor);

	return uri;
}

void
freeGPHDUri(GPHDUri *uri)
{
	GPHDUri_verify_type(uri);

	pfree(uri->protocol);
	GPHDUri_free_fragments(uri);

	if (uri->type == URI_GPHDFS)
	{
		pfree(uri->host);
		pfree(uri->port);
		pfree(uri->u.hdfs.directory);
		pfree(uri->u.hdfs.accessor);

		if(uri->u.hdfs.data_schema)
			pfree(uri->u.hdfs.data_schema);

		pfree(uri->u.hdfs.resolver);
	}
	else
	{
		pfree(uri->u.hbase.table_name);
	}

	pfree(uri);
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

	if (pg_strcasecmp(uri->protocol, "gpfusion") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid URI %s : unsupported protocol '%s'",
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
				 errmsg("invalid URI %s : missing authority section",
						 uri->uri)));
	}
	else
	{
		end = strchr(start, '/');

		if (!end)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid URI %s : missing authority section",
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
 * GPHDUri_parse_type
 *
 * parses the type section of the uri
 */
static void  
GPHDUri_parse_type(GPHDUri *uri, char **cursor)
{
	char	*start = *cursor;
	char	*end = strchr(start, '/');
	char	*type;
	int		 type_len;

	if (!end)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid URI %s: missing type ('hbase/hdfs')",
						uri->uri)));

	type_len = end - start;
	type = pnstrdup(start, type_len);

	if (pg_strcasecmp(type, "hdfs") == 0)
		uri->type = URI_GPHDFS;
	else if (pg_strcasecmp(type, "hbase") == 0)
		uri->type = URI_GPHBASE;
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid URI %s : unsupported type '%s'",
						uri->uri, type)));

	pfree(type);

	/* set cursor to skip type trailing '/' */
	*cursor = start + type_len + 1;
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
	char	*options_section = strchr(start, '?');
	size_t	 data_len;

	GPHDUri_verify_type(uri);

	/*
	 * If there exists an 'options' section, the data section length
	 * is from start point to options section point. Otherwise, the
	 * data section length is the remaining string length from start.
	 */
	if (options_section)
		data_len = options_section - start;
	else
		data_len = strlen(start);


	if (uri->type == URI_GPHDFS)
	{
		/* gphdfs: directory name */
		uri->u.hdfs.directory = pnstrdup(start, data_len);
	}
	else /* URI_GPHBASE */
	{
		/* gphbase: table name */
		uri->u.hbase.table_name = pnstrdup(start, data_len);
	}

	*cursor += data_len;
}

/*
 * GPHDUri_default_options
 *
 * Set the default options for each URI type.
 */
static void
GPHDUri_default_options(GPHDUri *uri)
{
	if (uri->type == URI_GPHBASE)
	{
		/* gphbase doesn't support options yet */
	}
	else /* URI_GPHDFS */
	{
		if(!uri->u.hdfs.accessor)
			uri->u.hdfs.accessor = pstrdup("SequenceFileAccessor");
		if(!uri->u.hdfs.data_schema)
			uri->u.hdfs.data_schema = pstrdup("schema.idl");
		if(!uri->u.hdfs.resolver)
			uri->u.hdfs.resolver = pstrdup("WritableResolver");
	}
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
	char	*start = *cursor;

	GPHDUri_verify_type(uri);
	GPHDUri_default_options(uri);

	/* option section must start with '?'. if absent, there are no options */
	if (start[0] != '?')
		return;

	/* skip '?' */
	start++;

	/* sanity check */
	if (strlen(start) < 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid %s URI %s: invalid option after '?'",
						(uri->type == URI_GPHDFS ? "hdfs" : "hbase"),
						uri->uri)));

	/* ok, parse the options now */
	if (uri->type == URI_GPHDFS)
	{
		const char	*sep = "&";
		char		*strtok_context;
		char		*pair;

		for (pair = strtok_r(start, sep, &strtok_context);
			 pair;
			 pair = strtok_r(NULL, sep, &strtok_context))
		{
			const char	*opt_accessor	= "accessor=";
			const char	*opt_schema		= "schema=";
			const char	*opt_resolver	= "resolver=";
			
			if (pg_strncasecmp(pair, opt_accessor, strlen(opt_accessor)) == 0)
			{
				const char	*value 		= pair + strlen(opt_accessor);

				pfree(uri->u.hdfs.accessor);
				uri->u.hdfs.accessor = pstrdup(value);
			}
			else if (pg_strncasecmp(pair, opt_schema, strlen(opt_schema)) == 0)
			{
				const char	*value 		= pair + strlen(opt_schema);

				pfree(uri->u.hdfs.data_schema);
				uri->u.hdfs.data_schema = pstrdup(value); /* keep case */
			}
			else if (pg_strncasecmp(pair, opt_resolver, strlen(opt_resolver)) == 0)
			{
				const char	*value 		= pair + strlen(opt_resolver);

				pfree(uri->u.hdfs.resolver);
				uri->u.hdfs.resolver = pstrdup(value);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("invalid gpfusion/hdfs URI %s: invalid option %s",
								uri->uri, pair)));
			}
		}
	}
	else /* URI_GPHBASE */
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid gpfusion/hbase URI %s: extra options after table "
						"name", uri->uri)));
	}
}

static void
GPHDUri_verify_type(GPHDUri *uri)
{
	if (uri->type != URI_GPHDFS && uri->type != URI_GPHBASE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Internal error in GPHDUri parser. Unsupported type")));
	}
}

/*
 * GPHDUri_debug_print
 *
 * For debugging while development only.
 */
void
GPHDUri_debug_print(GPHDUri *uri)
{
	bool	is_hdfs = (uri->type == URI_GPHDFS);

	GPHDUri_verify_type(uri);

	if (is_hdfs)
	{
		elog(NOTICE,
			 "Type: GPHDFS, Host: %s, Port: %s, Dir: %s, Type: %s, Idl: %s, Ser: %s",
			 uri->host,
			 uri->port,
			 uri->u.hdfs.directory,
			 uri->u.hdfs.accessor,
			 uri->u.hdfs.data_schema,
			 uri->u.hdfs.resolver);
	}
	else
	{
		elog(NOTICE,
			 "Type: GPHBASE, Host: %s, Port: %s, Table: %s",
			 uri->host,
			 uri->port,
			 uri->u.hbase.table_name);
	}

	GPHDUri_debug_print_segwork(uri);
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
		elog(NOTICE,
			 "%u: authority: %s, index %s",
			 count, data->authority, data->index);
		++count;
	}
}

/*
 * GPHDUri_parse_segwork parses the segwork section of the uri.
 * ...&segwork=<ip>@<port>@<index>+<ip>@<port>@<index>+...
 */
static void
GPHDUri_parse_segwork(GPHDUri *uri, const char *uri_str)
{
	const char	*sep = "+";
	char		*segwork;
	char		*strtok_context;
	char		*fragment;

	/* skip segwork= */
	segwork = strstr(uri_str, segwork_substring);
	if (segwork == NULL)
		return;

	segwork += strlen(segwork_substring);

	/* separate by - */
	for (fragment = strtok_r(segwork, sep, &strtok_context);
		 fragment;
		 fragment = strtok_r(NULL, sep, &strtok_context))
		uri->fragments = GPHDUri_parse_fragment(fragment, uri->fragments);
}

/*
 * Parsed a fragment string in the form:
 * <ip>@<port>@<index> - 192.168.1.1@1422@1
 * to authority ip:port - 192.168.1.1:1422
 * to index - 1
 */
static List* 
GPHDUri_parse_fragment(char* fragment, List* fragments)
{
	static const char	*sep = "@";
	char	*strtok_context;
	char	*value;
	StringInfoData formatter;
	FragmentData* fragment_data;

	fragment_data = palloc0(sizeof(FragmentData));
	initStringInfo(&formatter);

	/* expect ip */
	value = strtok_r(fragment, sep, &strtok_context);
	Assert(value != NULL);
	appendStringInfo(&formatter, "%s:", value);

	/* expect port */
	value = strtok_r(NULL, sep, &strtok_context);
	Assert(value != NULL);
	appendStringInfo(&formatter, "%s", value);
	fragment_data->authority = formatter.data;

	/* expect index */
	value = strtok_r(NULL, sep, &strtok_context);
	Assert(value != NULL);
	fragment_data->index = pstrdup(value);

	/* expect the end */
	value = strtok_r(NULL, sep, &strtok_context);
	Assert(value == NULL);

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
