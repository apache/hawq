/*-------------------------------------------------------------------------
 * pg_array.c
 *
 *      Functions for libpq array manipulation
 *      This is intended to contain functions to manage libpq arrays.
 *      This isn't likely to be comprehensive
 *
 *      Copyright (c) 2004-2007, PostgreSQL Global Development Group
 *      Author: Joshua Tolley
 *
 * $Id: pg_array.c,v 1.6 2007/09/13 14:20:43 h-saito Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "pg_array.h"

/* Text array format consists of leading and trailing curly braces. Values containing
 * characters that might be confusing (backslashes, double-quotes, etc.) are double-
 * quoted, and confusing characters are backslash-escaped */

/* pg_text_array_parse()
 * Takes an input string as returned by PQgetvalue(), parses it as a text array, and
 * builds an array of strings, which it returns, or NULL on error. pg_text_array also
 * sets len to the number of strings in the array. THE USER IS RESPONSIBLE
 * FOR FREEING CHAR ***OUTPUT, perhaps by calling pg_text_array_free. */

/* TODO: Add debugging/logging */

char ** pg_text_array_parse(char *input, int *len)
{
	char **results = NULL, **tmp;
	char *incursor, *outcursor, *output, *start_element;
	int escaped = 0, done_single_element = 0, done_all_elements = 0, i,
			cursorlen, inputlen, elements = 0, outputlen, error = 0, in_quotes =
					0;

	inputlen = strlen(input);
	if (!inputlen || inputlen < 3 || SIZE_MAX / inputlen < sizeof(char*))
		return NULL;
	incursor = input + 1;
	while (!done_all_elements)
	{
		in_quotes = 0;
		if (*incursor == '"')
		{
			incursor++;
			in_quotes = 1;
		}
		/*		else if (*incursor == ',') break; */
		start_element = incursor;
		outcursor = strpbrk(incursor, ",}");
		if (!outcursor)
			return NULL;
		/* FACT: outcursor - incursor >= 1 */
		outputlen = outcursor - incursor + 1;
		output = calloc(outputlen, sizeof(char));
		if (!output)
			return NULL;
		outcursor = output;
		done_single_element = 0;
		while (!done_single_element)
		{
			if (!escaped)
			{
				switch (*incursor)
				{
				case '}':
					done_single_element = 1;
					done_all_elements = 1;
					break;
				case ',':
					if (!in_quotes)
						done_single_element = 1;
					break;
				case '"':
					in_quotes = 0;
					done_single_element = 1;
					incursor++;
					if (*incursor == '}')
						done_all_elements = 1;
					break;
				}
			}
			if (!done_single_element)
			{
				if (!escaped && *incursor == '\\')
					escaped = 1;
				else
				{
					if (outcursor - output >= outputlen)
					{
						cursorlen = outcursor - output;
						outcursor = strpbrk(incursor + 1, ",}");
						if (!outcursor)
						{
							error = 1;
							break;
						}
						outputlen = outcursor - start_element;
						outcursor = realloc(output, outputlen * sizeof(char));
						if (!outcursor)
						{
							error = 1;
							break;
						}
						output = outcursor;
						outcursor = output + cursorlen;
					}
					if (escaped)
					{
						escaped = 0;
						*outcursor = *incursor;
						outcursor++;
					}
					else
					{
						*outcursor = *incursor;
						outcursor++;
					}
				}
			}
			if (done_single_element)
			{
				elements++;
				memset(outcursor, 0, outputlen - (outcursor - output));
				if (results)
				{
					tmp = realloc(results, elements * sizeof(char*));
					if (!tmp)
					{
						error = 1;
						break;
					}
					results = tmp;
				}
				else
				{
					results = calloc(1, sizeof(char*));
					if (!results)
					{
						error = 1;
						break;
					}
				}
				results[elements - 1] = output;
			}
			if (!done_all_elements)
			{
				incursor++;
				if (incursor - input >= inputlen)
				{
					error = 1;
					break;
				}
			}
		}
		if (error)
			break;
	}
	if (error)
	{
		for (i = 0; i < elements; i++)
			free(results[i]);
		free(results);
		free(output);
		return NULL;
	}
	*len = elements;
	return results;
}

/* pg_text_array_free() 
 * Frees an array of strings such as is returned by pg_text_array_parse() */
void pg_text_array_free(char **array, int len)
{
	int i;
	for (i = 0; i < len; i++)
		free(array[i]);
	free(array);
}
