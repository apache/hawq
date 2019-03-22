#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

class type:

    def __init__(self, name, symbol, sqltype, typekind):
        self.name = name
        self.symbol = symbol
        self.sqltype = sqltype
        self.typekind = typekind

(
    BOOLEAN,
    STRING,
    BPCHAR,
    BINARY,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    INTERVAL
) = range(15)

BOOL_TYPE = {
    BOOLEAN: type("boolean", "bool", "BOOLEAN", "BOOLEANID")
}

STRING_TYPES = {
    STRING: type("string", "char *", "STRING", "STRINGID")
}

BPCHAR_TYPES = {
    BPCHAR: type("bpchar", "char *", "BPCHAR", "CHARID")
}

BINARY_TYPES = {
    BINARY: type("binary", "char *", "BINARY", "BINARYID")
}

DECIMAL_TYPE = {
	DECIMAL: type("decimal", "double", "DECIMAL", "DECIMALNEWID")
}

INT_TYPES = {
    TINYINT: type("int8", "int8_t", "TINYINT", "TINYINTID"),
    SMALLINT: type("int16", "int16_t", "SMALLINT", "SMALLINTID"),
    INT: type("int32", "int32_t", "INT", "INTID"),
    BIGINT: type("int64", "int64_t", "BIGINT", "BIGINTID")
}

FLOAT_TYPES = {
    FLOAT: type("float", "float", "FLOAT", "FLOATID"),
    DOUBLE: type("double", "double", "DOUBLE", "DOUBLEID")
}

DATE_TYPES = {
    DATE: type("date", "int32_t", "DATE", "DATE"),
    TIME: type("time", "int64_t", "TIME", "TIME")
}

TIMESTAMP_TYPE = {
    TIMESTAMP: type("timestamp", "int64_t", "TIMESTAMP", "TIMESTAMPID")
}

INTERVAL_TYPE = {
	INTERVAL: type("interval", "int64_t", "INTERVAL", "INTERVALID")
}

NUMERIC_TYPES = dict(INT_TYPES.items() + FLOAT_TYPES.items())
NUMERIC_AND_DECIMAL_TYPES = dict(INT_TYPES.items() + FLOAT_TYPES.items() + DECIMAL_TYPE.items())

ALL_TYPES = dict(
    INT_TYPES.items() +
    FLOAT_TYPES.items() +
    DECIMAL_TYPE.items() +
    STRING_TYPES.items() +
    BOOL_TYPE.items() +
    DATE_TYPES.items() +
    TIMESTAMP_TYPE.items() +
    INTERVAL_TYPE.items())


class operator:

    def __init__(self, name, symbol, sqlsym, negname, oprcom, oprrest, oprjoin, uppername, functionobj=""):
        self.name = name
        self.symbol = symbol
        self.sqlsym = sqlsym
        self.negname = negname
        self.oprcom = oprcom
        self.oprrest = oprrest
        self.oprjoin = oprjoin
        self.uppername = uppername
        self.functionobj = functionobj

(
    LESS,
    LESS_EQ,
    EQUAL,
    NOT_EQUAL,
    GREATER,
    GREATER_EQ,
    ADD,
    SUB,
    MUL,
    DIV,
    MOD,
    AND,
    OR,
    NOT,
    SUM,
    COUNT,
    AVG,
    MIN,
    MAX
) = range(19)

CMP_OP = {
    LESS: operator("less_than", "<", "<", "greater_eq", "greater_than", "SCALARLTSEL", "SCALARLTJOINSEL", "LESS_THAN", "less"),
    LESS_EQ: operator("less_eq", "<=", "<=", "greater_than", "greater_eq", "SCALARLTSEL", "SCALARLTJOINSEL", "LESS_EQ", "less_equal"),
    EQUAL: operator("equal", "==", "=", "not_equal", "equal", "EQSEL", "EQJOINSEL", "EQUAL", "equal_to"),
    NOT_EQUAL: operator("not_equal", "!=", "<>", "equal", "not_equal", "NEQSEL", "NEQJOINSEL", "NOT_EQUAL", "not_equal_to"),
    GREATER: operator("greater_than", ">", ">", "less_eq", "less_than", "SCALARGTSEL", "SCALARGTJOINSEL", "GREATER_THAN", "greater"),
    GREATER_EQ: operator("greater_eq", ">=", ">=", "less_than", "less_eq", "SCALARGTSEL", "SCALARGTJOINSEL", "GREATER_EQ", "greater_equal")
}

ARITH_OP = {
    ADD: operator("add", "+", "+", "", "", "", "", "ADD", "plus"),
    SUB: operator("sub", "-", "-", "", "", "", "", "SUB", "minus"),
    MUL: operator("mul", "*", "*", "", "", "", "", "MUL", "multiplies"),
    DIV: operator("div", "/", "/", "", "", "", "", "DIV", "divides")
}

MOD_OP = {
    MOD: operator("add", "%", "%", "", "", "", "", "ADD")
}

LOGIC_OP = {
    AND: operator("logic_and", "&&", "", "", "", "", "", "LOGIC_AND"),
    OR: operator("logic_or", "||", "", "", "", "", "", "LOGIC_OR"),
    NOT: operator("logic_not", "!", "", "", "", "", "", "LOGIC_NOT"),
}


def get_func_return_type(ltype, rtype, op):
    if op in CMP_OP:
        return ALL_TYPES[BOOLEAN]
    elif op in ARITH_OP:
        ret = ltype if ltype > rtype else rtype
        return ALL_TYPES[ret]


class aggregate:

    def __init__(self, name, anytype, rettype, transname, prelimname, finname):
        self.name = name
        self.anytype = anytype
        self.rettype = rettype  # for any type input
        self.transname = transname
        self.prelimname = prelimname
        self.finname = finname


def get_agg_func_id(agg, type):
    (funcname, transname, prelimname, finname) = (None, None, None, None)

    if agg.anytype:
        funcname = agg.name
        transname = agg.name + "_" + agg.transname
        prelimname = agg.name + "_" + agg.prelimname
        if agg.finname is not None:
            finname = agg.name + "_" + agg.finname
    else:
        funcname = agg.name + "_" + type.sqltype
        transname = agg.name + "_" + type.sqltype + "_" + agg.transname
        prelimname = agg.name + "_" + type.sqltype + "_" + agg.prelimname
        if agg.finname is not None:
            finname = agg.name + "_" + type.sqltype + "_" + agg.finname

    funcname = funcname.upper()
    transname = transname.upper()
    prelimname = prelimname.upper()
    finname = None if finname is None else finname.upper()

    return (funcname, transname, prelimname, finname)

MIN_AGG = aggregate("min", False, None, "smaller", "smaller", None)
MAX_AGG = aggregate("max", False, None, "larger", "larger", None)
SUM_AGG = aggregate("sum", False, None, "sum", "add", None)
COUNT_AGG = aggregate("count", True, "BIGINT", "inc", "add", None)
AVG_AGG = aggregate("avg", False, None, "accu", "amalg", "avg")

ALL_AGGS = {
    SUM: SUM_AGG,
    COUNT: COUNT_AGG,
    AVG: AVG_AGG,
    MIN: MIN_AGG,
    MAX: MAX_AGG
}

MIN_MAX_AGGS = {
  MIN : MIN_AGG, MAX : MAX_AGG
}
