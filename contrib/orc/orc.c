#include <json-c/json.h>

#include "c.h"
#include "port.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "utils/hawq_type_mapping.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/uri.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/datetime.h"
#include "mb/pg_wchar.h"
#include "commands/defrem.h"
#include "commands/copy.h"
#include "access/tupdesc.h"
#include "access/filesplit.h"
#include "access/fileam.h"
#include "access/plugstorage.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_exttable.h"
#include "catalog/namespace.h"
#include "postmaster/identity.h"
#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "utils/uri.h"
#include "cdb/cdbfilesystemcredential.h"

#include "storage/cwrapper/orc-format-c.h"
#include "storage/cwrapper/hdfs-file-system-c.h"
#include "cdb/cdbvars.h"
#define ORC_TIMESTAMP_EPOCH_JDATE 2457024 /* == date2j(2015, 1, 1) */
#define MAX_ORC_ARRAY_DIMS        10000
#define ORC_NUMERIC_MAX_PRECISION 38

/* Do the module magic dance */
PG_MODULE_MAGIC
;

/* Validators for pluggable storage format ORC */
PG_FUNCTION_INFO_V1(orc_validate_interfaces);
PG_FUNCTION_INFO_V1(orc_validate_options);
PG_FUNCTION_INFO_V1(orc_validate_encodings);
PG_FUNCTION_INFO_V1(orc_validate_datatypes);

/* Implementation of validators for pluggable storage format ORC */

/*
 * void
 * orc_validate_interfaces(char *formatName)
 */
Datum orc_validate_interfaces(PG_FUNCTION_ARGS)
{
  PlugStorageValidator psv_interface =
      (PlugStorageValidator) (fcinfo->context);

  if (pg_strncasecmp(psv_interface->format_name, "orc", strlen("orc")) != 0)
  {
    ereport(ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("orc_validate_interface : incorrect format name \'%s\'", psv_interface->format_name)));
  }

  PG_RETURN_VOID() ;
}

/*
 * void
 * orc_validate_options(List *formatOptions,
 *                      char *formatStr,
 *                      bool isWritable)
 */
Datum orc_validate_options(PG_FUNCTION_ARGS)
{
  PlugStorageValidator psv = (PlugStorageValidator) (fcinfo->context);

  List *format_opts = psv->format_opts;
  char *format_str = psv->format_str;
  bool is_writable = psv->is_writable;
  TupleDesc tup_desc = psv->tuple_desc;

  char *formatter = NULL;
  char *compresstype = NULL;
  char *bloomfilter = NULL;
  char *dicthreshold = NULL;
  char *bucketnum = NULL;
  char *category = NULL;

  ListCell *opt;

  const int maxlen = 8 * 1024 - 1;
  int len = 0;

  foreach(opt, format_opts)
  {
    DefElem *defel = (DefElem *) lfirst(opt);
    char *key = defel->defname;
    bool need_free_value = false;
    char *val = (char *) defGetString(defel, &need_free_value);

    /* check formatter */
    if (strncasecmp(key, "formatter", strlen("formatter")) == 0)
    {
      char *formatter_values[] =
      { "orc" };
      checkPlugStorageFormatOption(&formatter, key, val,
      true, 1, formatter_values);
    }

    /* check option for orc format */
    if (strncasecmp(key, "compresstype", strlen("compresstype")) == 0)
    {
      char *compresstype_values[] =
      { "none", "snappy", "lz4" };
      checkPlugStorageFormatOption(&compresstype, key, val, is_writable,
          3, compresstype_values);
    }

    if (strncasecmp(key, "bloomfilter", strlen("bloomfilter")) == 0)
    {
      int attnum = tup_desc->natts;
      char **attribute_names = palloc0(attnum * sizeof(char*));
      for (int i = 0; i < attnum; ++i) {
        int name_len = strlen(((Form_pg_attribute) (tup_desc->attrs[i]))->attname.data);
        char *attribute = palloc0(name_len + 1);
        strncpy(attribute, ((Form_pg_attribute) (tup_desc->attrs[i]))->attname.data, name_len);
        attribute_names[i] = attribute;
      }
      char *dup_val = pstrdup(val);
      char *token = strtok(dup_val, ",");
      while (token) {
        checkPlugStorageFormatOption(&bloomfilter, key, token, true, attnum, attribute_names);
        bloomfilter = NULL;
        token = strtok(NULL, ",");
      }
    }

    if (strncasecmp(key, "dicthreshold", strlen("dicthreshold")) == 0)
    {
      checkPlugStorageFormatOption(&dicthreshold, key, val,
      true, 0, NULL);
      char *end;
      double threshold = strtod(val, &end);
      if (end == val || *end != '\0' || threshold < 0 || threshold > 1)
      {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("dicthreshold \"%s\" must be within [0-1]", val), errOmitLocation(true)));
      }
    }

    if (strncasecmp(key, "bucketnum", strlen("bucketnum")) == 0)
    {
      checkPlugStorageFormatOption(&bucketnum, key, val,
      true, 0, NULL);
      char *end;
      long bucketnumber = strtol(val, &end, 10);
      if (end == val || *end != '\0' || bucketnumber <= 0)
      {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("bucketnum \"%s\" must be > 0", val), errOmitLocation(true)));
      }
    }

    /* check category orc format */
    if (strncasecmp(key, "category", strlen("category")) == 0)
    {
      char *category_values[] =
      { "internal", "external" };
      checkPlugStorageFormatOption(&category, key, val,
      true, 2, category_values);
    }

    if (strncasecmp(key, "formatter", strlen("formatter"))
        && strncasecmp(key, "compresstype", strlen("compresstype"))
        && strncasecmp(key, "bloomfilter", strlen("bloomfilter"))
        && strncasecmp(key, "dicthreshold", strlen("dicthreshold"))
        && strncasecmp(key, "bucketnum", strlen("bucketnum"))
        && strncasecmp(key, "category", strlen("category")))
    {
      ereport(ERROR,
          (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Option \"%s\" for ORC table is invalid", key), errhint("Format options for ORC table must be either " "formatter, compresstype, bloomfilter or dicthreshold"), errOmitLocation(true)));
    }

    sprintf((char * ) format_str + len, "%s '%s' ", key, val);
    len += strlen(key) + strlen(val) + 4;

    if (need_free_value)
    {
      pfree(val);
      val = NULL;
    }

    AssertImply(need_free_value, NULL == val);

    if (len > maxlen)
    {
      ereport(ERROR,
          (errcode(ERRCODE_SYNTAX_ERROR), errmsg("format options must be less than %d bytes in size", maxlen), errOmitLocation(true)));
    }
  }

  if (!formatter)
  {
    ereport(ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("no formatter function specified"), errOmitLocation(true)));
  }

  PG_RETURN_VOID() ;
}

/*
 * void
 * orc_validate_encodings(char *encodingName)
 */
Datum orc_validate_encodings(PG_FUNCTION_ARGS)
{
  PlugStorageValidator psv = (PlugStorageValidator) (fcinfo->context);
  char *encoding_name = psv->encoding_name;

  if (strncasecmp(encoding_name, "utf8", strlen("utf8")))
  {
    ereport(ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("\"%s\" is not a valid encoding for ORC external table. " "Encoding for ORC external table must be UTF8.", encoding_name), errOmitLocation(true)));
  }

  PG_RETURN_VOID() ;
}

/*
 * void
 * orc_validate_datatypes(TupleDesc tupDesc)
 */
Datum orc_validate_datatypes(PG_FUNCTION_ARGS)
{
  PlugStorageValidator psv = (PlugStorageValidator) (fcinfo->context);
  TupleDesc tup_desc = psv->tuple_desc;

  for (int i = 0; i < tup_desc->natts; ++i)
  {
    int32_t datatype =
        (int32_t) (((Form_pg_attribute) (tup_desc->attrs[i]))->atttypid);

    if (checkORCUnsupportedDataType(datatype))
    {
      ereport(ERROR,
          (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unsupported data types %d for columns of external ORC table is specified.", datatype), errOmitLocation(true)));
    }
    /*
     * TODO(wshao): additional check for orc decimal type
     * orc format currently does not support decimal precision larger than 38
     */
  }

  PG_RETURN_VOID() ;
}
