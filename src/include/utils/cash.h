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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * cash.h
 * Written by D'Arcy J.M. Cain
 *
 * Functions to allow input and output of money normally but store
 *	and handle it as 64 bit integer.
 */

#ifndef CASH_H
#define CASH_H

#include "fmgr.h"

typedef int64 Cash;

extern Datum cash_in(PG_FUNCTION_ARGS);
extern Datum cash_out(PG_FUNCTION_ARGS);
extern Datum cash_recv(PG_FUNCTION_ARGS);
extern Datum cash_send(PG_FUNCTION_ARGS);

extern Datum cash_eq(PG_FUNCTION_ARGS);
extern Datum cash_ne(PG_FUNCTION_ARGS);
extern Datum cash_lt(PG_FUNCTION_ARGS);
extern Datum cash_le(PG_FUNCTION_ARGS);
extern Datum cash_gt(PG_FUNCTION_ARGS);
extern Datum cash_ge(PG_FUNCTION_ARGS);
extern Datum cash_cmp(PG_FUNCTION_ARGS);

extern Datum cash_pl(PG_FUNCTION_ARGS);
extern Datum cash_mi(PG_FUNCTION_ARGS);

extern Datum cash_mul_flt8(PG_FUNCTION_ARGS);
extern Datum cash_div_flt8(PG_FUNCTION_ARGS);
extern Datum flt8_mul_cash(PG_FUNCTION_ARGS);

extern Datum cash_mul_flt4(PG_FUNCTION_ARGS);
extern Datum cash_div_flt4(PG_FUNCTION_ARGS);
extern Datum flt4_mul_cash(PG_FUNCTION_ARGS);

extern Datum cash_mul_int8(PG_FUNCTION_ARGS);
extern Datum int8_mul_cash(PG_FUNCTION_ARGS);
extern Datum cash_div_int8(PG_FUNCTION_ARGS);

extern Datum cash_mul_int4(PG_FUNCTION_ARGS);
extern Datum cash_div_int4(PG_FUNCTION_ARGS);
extern Datum int4_mul_cash(PG_FUNCTION_ARGS);

extern Datum cash_mul_int2(PG_FUNCTION_ARGS);
extern Datum int2_mul_cash(PG_FUNCTION_ARGS);
extern Datum cash_div_int2(PG_FUNCTION_ARGS);

extern Datum cashlarger(PG_FUNCTION_ARGS);
extern Datum cashsmaller(PG_FUNCTION_ARGS);

extern Datum cash_words(PG_FUNCTION_ARGS);

#endif   /* CASH_H */
