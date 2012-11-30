#ifndef CUSTHANDLER_H
#define CUSTHANDLER_H

#include "gpsnmpd.h"

void init_custom_queries(void);

typedef struct pgsnmpd_query pgsnmpd_query;

extern char *custom_query_config_file;

struct pgsnmpd_query {
    char  *table_name;
    char  *query_text;
    oid   *table_oid;
    size_t oid_len;

    int      num_indexes;
    int      min_colnum;
    u_char  *types;
    int      typeslen;

    /* Number of rows and columns returned by the query */
    int rowcount;
    int colcount;

    PGresult *result;

    /* How long (sec) before I need to refresh this result */
    int cache_timeout;
    /* When the result was last refreshed */
    time_t last_refresh;

    netsnmp_table_array_callbacks cb;
    netsnmp_handler_registration *my_handler;

    pgsnmpd_query *next;
};

pgsnmpd_query *alloc_custom_query(char *table_name, char *query_text,
                                    oid *table_oid, int oid_len);

#endif /* CUSTHANDLER_H */
