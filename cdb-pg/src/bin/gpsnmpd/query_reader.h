#ifndef QUERY_READER_H
#define QUERY_READER_H

#include "customquery.h"

pgsnmpd_query *
parse_config(const char *filename);

#endif  /* QUERY_READER_H */
