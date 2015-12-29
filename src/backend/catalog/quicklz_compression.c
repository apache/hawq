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
 *
 * ---------------------------------------------------------------------
 *
 * The quicklz implementation is not provided due to licensing issues.  The
 * following stub implementation is built if a proprietary implementation is
 * not provided.
 *
 * Note that other compression algorithms actually work better for new
 * installations anyway.
 */

#include "postgres.h"
#include "utils/builtins.h"

Datum
quicklz_constructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_destructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_compress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_decompress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_validator(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}


