#ifndef _GPFUSION_URIPARSER_H_
#define _GPFUSION_URIPARSER_H_

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

/* -- URI parsing -- */

/*
 * HadoopUriType - currently supported gp hadoop uri's
 */
typedef enum HadoopUriType
{
	URI_GPHDFS,
	URI_GPHBASE

} HadoopUriType;

/*
 * FragmentData - describes a single Hadoop file split / HBase table region
 * in means of location (ip, port) and index of a splits/regions list
 */
typedef struct FragmentData
{
	char	 *authority;
	char	 *index;
} FragmentData;

/*
 * GPHDUri - Describes the contents of a hadoop (gphdfs or gphbase) uri.
 */
typedef struct GPHDUri
{

    HadoopUriType 	 type;		/* currently: hdfs or hbase */

    /* common */
    char 			*uri;		/* the unparsed user uri	*/
	char			*protocol;	/* the protocol name		*/
	char			*host;		/* host name str			*/
	char			*port;		/* port number as string	*/
	List			*fragments; /* list of FragmentData		*/


	/* type specific */
	union
	{
		struct
		{
			char	*directory;
			char 	*accessor;
			char	*resolver;
			char	*data_schema;
		} hdfs;

		struct
		{
			char	*table_name;
		} hbase;
	} u;

} GPHDUri;

GPHDUri	*parseGPHDUri(const char *uri_str);
void 	 freeGPHDUri(GPHDUri *uri);
void	 GPHDUri_debug_print(GPHDUri *uri);

#endif	// 
