#ifndef _PXF_URIPARSER_H_
#define _PXF_URIPARSER_H_

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

/*
 * Path constants for accessing GPFX.
 * All PXF's resources are under /GPDB_REST_PREFIX/PXF_VERSION/...
 */
#define GPDB_REST_PREFIX "gpdb"
#define PXF_VERSION "v8" /* PXF version */

/*
 * FragmentData - describes a single Hadoop file split / HBase table region
 * in means of location (ip, port), the source name of the specific file/table that is being accessed,
 * and the index of a of list of fragments (splits/regions) for that source name.
 * The index refers to the list of the fragments of that source name.
 * user_data is optional.
 */
typedef struct FragmentData
{
	char	 *authority;
	char	 *index;
	char	 *source_name;
	char	 *user_data;
} FragmentData;

typedef struct OptionData
{
	char	 *key;
	char	 *value;
} OptionData;

/*
 * GPHDUri - Describes the contents of a hadoop uri.
 */
typedef struct GPHDUri
{

    /* common */
    char 			*uri;		/* the unparsed user uri	*/
	char			*protocol;	/* the protocol name		*/
	char			*host;		/* host name str			*/
	char			*port;		/* port number as string	*/
	char			*data;      /* data location (path)     */
	List			*fragments; /* list of FragmentData		*/

	/* options */
	List			*options;   /* list of OptionData 		*/

} GPHDUri;

GPHDUri	*parseGPHDUri(const char *uri_str);
void 	 freeGPHDUri(GPHDUri *uri);
void	 GPHDUri_debug_print(GPHDUri *uri);
int		 GPHDUri_get_value_for_opt(GPHDUri *uri, char *key, char **val, bool emit_error);
bool 	 RelationIsExternalPxfReadOnly(Relation rel, StringInfo location);
void 	 GPHDUri_verify_no_duplicate_options(GPHDUri *uri);
void 	 GPHDUri_verify_core_options_exist(GPHDUri *uri, List *coreOptions);

#endif	// _PXF_URIPARSER_H_
