#!/usr/bin/env python
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

import re
import os

class PgCatalogHeader(object):

    """This class is a base class for catalog header parser class, and
    provides basic methods to parse header files by regular expressions.
    The result will be in self.tuplist.  To extend this class, set these
    three class values.
    - header
    - hasoid
    - prefix
    and call self.initialize() in __init__().

    """
    catalogdir = '../../../include/catalog'

    def initialize(self):
        path = self.fullpath(self.header)
        self.tuplist = self.readheader(path, self.hasoid, self.prefix)
        if not self.tuplist:
            raise Exception("no content")

    def fullpath(self, filename):
        """Returns the full path name of the catalog file."""

        thisdir = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(thisdir, self.catalogdir)
        return os.path.join(path, filename)

    def readheader(self, header, hasoid, tableprefix):
        """Returns a list of dictionaries as the result of parse.
        It finds lines starting with "#define Anum_" and collects
        attribute names, then parse DATA() macros.  All data is
        parsed as string regardless of the column type.

        """
        anum = re.compile(r'#define Anum_' + tableprefix + r'_(\w+)')
        rebuf = list()
        attlist = list()
        for line in open(header):
            m = anum.match(line)
            if m:
                # Build up regular expression.
                # We capture the group by name to look up later
                rebuf.append(r'(?P<' + m.group(1) + r'>\S+|"[^"]+")')
                attlist.append(m.group(1))
        oidpattern = ''
        if hasoid:
            oidpattern = r'OID\s*=\s*(?P<oid>\w+)\s*'
            attlist.append('oid')
        insert = re.compile(r'DATA\(insert\s+' +
                            oidpattern + r'\(\s*' +
                            '\s+'.join(rebuf) +
                            r'\s*\)\);')

        # Collect all the DATA() lines and put them into a list
        tuplist = list()
        for line in open(header):
            m = insert.match(line)
            if m:
                tup = dict()
                for att in attlist:
                    tup[att] = m.group(att)
                tuplist.append(tup)
        return tuplist

class PgAmop(PgCatalogHeader):

    header = 'pg_amop.h'
    hasoid = False
    prefix = 'pg_amop'

    def __init__(self):
        self.initialize()

    def find_amopopr(self, amopclaid, amopstrategy):
        """Returns the operator oid that matches opclass and strategy."""
        for tup in self.tuplist:
            if (tup['amopclaid'] == str(amopclaid) and
                tup['amopstrategy'] == str(amopstrategy)):
                return tup['amopopr']

class PgOpclass(PgCatalogHeader):

    header = 'pg_opclass.h'
    hasoid = True
    prefix = 'pg_opclass'

    def __init__(self):
        self.initialize()

    def find_btree_oid_by_opcintype(self, opcintype):
        """Returns the opclass oid whoose input type is opcintype if it
        is a btree opclass and default for the type.

        """
        for tup in self.tuplist:
            # 403 is the btree access method id
            if (tup['opcintype'] == str(opcintype) and
                tup['opcamid'] == '403' and
                tup['opcdefault'] == 't'):
                return tup['oid']


class PgOperator(PgCatalogHeader):

    header = 'pg_operator.h'
    hasoid = True
    prefix = 'pg_operator'

    def __init__(self):
        self.initialize()

    def find_oprcode(self, oid):
        """Returns the procedure oid of the operator."""
        for tup in self.tuplist:
            if tup['oid'] == str(oid):
                return tup['oprcode']

class PgType(PgCatalogHeader):

    header = 'pg_type.h'
    hasoid = True
    prefix = 'pg_type'

    def __init__(self):
        self.initialize()
        self.oid_defs = self._read_oid_defs()

    def findtup_by_typname(self, typname):
        """Returns a tuple that matches typname.
        The input typname is normalized if it's any of quote_char, boolean,
        smallint, integer, bigint, real, or timestamp_with_time_zone.
        Also, if typname looks like an array type with '[]', it is normalized
        to an array type name with underscore prefix.

        """
        basename = typname.rstrip('[]')
        isarray = False

        if basename != typname:
            isarray = True
            typname = basename

        if typname == 'quoted_char':
            typname = 'char'
        elif typname == 'boolean':
            typname = 'bool'
        elif typname == 'smallint':
            typname = 'int2'
        elif typname == 'integer':
            typname = 'int4'
        elif typname == 'bigint':
            typname = 'int8'
        elif typname == 'real':
            typname = 'float4'
        elif typname == 'timestamp_with_time_zone':
            typname = 'timestamptz'

        if isarray:
            typname = '_' + typname

        for tup in self.tuplist:
            if tup['typname'] == str(typname):
                return tup

    def findtup_by_typid(self, typid):
        for tup in self.tuplist:
            if tup['oid'] == str(typid):
                return tup

    def oid_to_def(self, oid):
        return self.oid_defs.get(int(oid), str(oid))

    def _read_oid_defs(self):
        """Reads #define lines in pg_type.sql and builds up a map from
        oid(int) to macro string.
        """
        filename = os.path.join(self.catalogdir, 'pg_type.sql')
        pat = re.compile(r'^.*#define\s+\S*OID\s+\d+')
        oidmap = dict()
        for line in open(filename):
            m = pat.match(line)
            if m:
                tup = line.split()
                oid = int(tup[-1])
                oidname = tup[-2]
                oidmap[oid] = oidname

        return oidmap

class PgProc(PgCatalogHeader):

    header = 'pg_proc.h'
    hasoid = True
    prefix = 'pg_proc'

    def __init__(self):
        self.initialize()

    def find_prosrc_by_proname(self, proname):
        for tup in self.tuplist:
            if tup['proname'] == str(proname):
                return tup['prosrc']
