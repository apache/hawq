/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * $PostgreSQL: pgsql/src/test/locale/test-ctype.c,v 1.6 2009/06/11 14:49:15 momjian Exp $
 */

/*

   test-ctype.c

Written by Oleg BroytMann, phd2@earthling.net
   with help from Oleg Bartunov, oleg@sai.msu.su
Copyright (C) 1998 PhiloSoft Design

This is copyrighted but free software. You can use it, modify and distribute
in original or modified form providing that the author's names and the above
copyright notice will remain.

Disclaimer, legal notice and absence of warranty.
   This software provided "as is" without any kind of warranty. In no event
the author shall be liable for any damage, etc.

*/

#include <stdio.h>
#include <locale.h>
#include <ctype.h>

char	   *flag(int b);
void		describe_char(int c);

#undef LONG_FLAG

char *
flag(int b)
{
#ifdef LONG_FLAG
	return b ? "yes" : "no";
#else
	return b ? "+" : " ";
#endif
}

void
describe_char(int c)
{
	unsigned char cp = c,
				up = toupper(c),
				lo = tolower(c);

	if (!isprint(cp))
		cp = ' ';
	if (!isprint(up))
		up = ' ';
	if (!isprint(lo))
		lo = ' ';

	printf("chr#%-4d%2c%6s%6s%6s%6s%6s%6s%6s%6s%6s%6s%6s%4c%4c\n", c, cp, flag(isalnum(c)), flag(isalpha(c)), flag(iscntrl(c)), flag(isdigit(c)), flag(islower(c)), flag(isgraph(c)), flag(isprint(c)), flag(ispunct(c)), flag(isspace(c)), flag(isupper(c)), flag(isxdigit(c)), lo, up);
}

int
main()
{
	short		c;
	char	   *cur_locale;

	cur_locale = setlocale(LC_ALL, "");
	if (cur_locale)
		fprintf(stderr, "Successfulle set locale to %s\n", cur_locale);
	else
	{
		fprintf(stderr, "Cannot setup locale. Either your libc does not provide\nlocale support, or your locale data is corrupt, or you have not set\nLANG or LC_CTYPE environment variable to proper value. Program aborted.\n");
		return 1;
	}

	printf("char#  char alnum alpha cntrl digit lower graph print punct space upper xdigit lo up\n");
	for (c = 0; c <= 255; c++)
		describe_char(c);

	return 0;
}
