#ifndef PG_ARRAY_H
#define PG_ARRAY_H

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

char **pg_text_array_parse(char* input, int *len);
void pg_text_array_free(char **array, int len);

#endif
