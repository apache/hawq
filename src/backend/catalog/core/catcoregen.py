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

# -----------------------------------------------------------------
# catcoregen.py
# Usage:
#   $ catcoregen.py OUTNAME JSONPATH
#
# Where OUTNAME is the output C file name, and JSONPATH is the
# input catdump json path.
#
# This script generates CatCore C definitions from catdump json.
# The structure looks like:
#
# - array of relation
#   - array of attribute
#   - array of index
# - array of type
#
# See also catcore.h for each structure definition.
# -----------------------------------------------------------------
from __future__ import print_function
import datetime
import json
import re
import os
import sys

import catheader

# When changes this, you should change it in catcore.h, too.
MAX_SCAN_NUM = 4

class CatDumpReader(object):
    """This is the class to access catdump json content.  It is aimed for
    general purposes but for now we have it here in the same file.

    """
    def __init__(self, json):
        self.json = json
        self.pg_amop = catheader.PgAmop()
        self.pg_opclass = catheader.PgOpclass()
        self.pg_operator = catheader.PgOperator()
        self.pg_type = catheader.PgType()
        self.pg_proc = catheader.PgProc()

        self._add_info()

    def _add_info(self):
        """A private method to add more info in json tree."""

        # Add type oid to column dictionaries.
        for relname in self.get_relnames():
            cols = self.get_cols_by_relname(relname)
            for i, col in enumerate(cols):
                sqltype = col['sqltype']
                typid = self.sqltype_typid(sqltype)
                col['atttypid'] = typid
                col['atttypiddef'] = self.pg_type.oid_to_def(typid)
                col['attnum'] = i + 1

        # To speed up typid_pos
        type_pos_map = {}
        for i, typid in enumerate(self.unique_typids_sorted()):
            type_pos_map[int(typid)] = i
        self._type_pos_map = type_pos_map

        # To speed up index_pos
        index_pos_map = {}
        for relname in self.get_relnames():
            indexes = self.get_indexes_by_relname(relname)
            for i, index in enumerate(indexes):
                cols = [col[0] for col in index['cols']]
                index_pos_map[(relname, ','.join(cols))] = i
        self._index_pos_map = index_pos_map

    def get_relnames(self):
        return [x for x in self.json.keys() if not x.startswith('__')]

    def CamelCaseRelationId(self, relname):
        ident = self.json[relname]['CamelCaseRelationId']
        # some relation id names are broken
        ident = re.sub(r'_([a-z])', lambda p: p.group(1).upper(), ident)
        if ident == 'GpVersionAtInitdbRelationId':
            ident = 'GpVersionRelationId'
        return ident

    def get_cols_by_relname(self, relname):
        return self.json[relname]['cols']

    def get_indexes_by_relname(self, relname):
        return self.json[relname].get('indexes', [])

    def get_fklist_by_relname(self, relname):
        return self.json[relname].get('fk_list', [])

    def attname_to_anum(self, relname, attname):
        """Returns postgres attribute number definition such like
        Anum_pg_class_relid, from attribute name.

        """
        if attname == 'oid':
            return 'ObjectIdAttributeNumber'

        if relname == 'gp_distribution_policy':
            relname = 'gp_policy'
        elif relname == 'pg_stat_last_operation':
            relname = 'pg_statlastop'
        elif relname == 'pg_stat_last_shoperation':
            relname = 'pg_statlastshop'
        return 'Anum_{relname}_{attname}'.format(
                    relname=relname, attname=attname)

    def rel_has_oid(self, relname):
        if self.json[relname]["with"]["oid"]:
            return True
        else:
            return False

    def unique_typids(self):
        typeset = set()
        for relname in self.get_relnames():
            for col in self.get_cols_by_relname(relname):
                sqltype = col['sqltype']
                typeset.add(self.sqltype_typid(sqltype))
        return typeset

    def unique_typids_sorted(self):
        return sorted(self.unique_typids(), key=lambda x: int(x))

    def typid_pos(self, typid):
        return self._type_pos_map[int(typid)]

    def index_pos(self, relname, cols):
        return self._index_pos_map.get((relname, ','.join(cols)), -1)

    def sqltype_typid(self, sqltype):
        tup = self.pg_type.findtup_by_typname(sqltype)
        if tup is None:
            raise Exception('{sqltype} is not found'.format(sqltype=sqltype))
        return tup['oid']

    def typid_sqltype(self, typid):
        tup = self.pg_type.findtup_by_typid(typid)
        return tup['typname']

    def btree_ops(self, typid):
        """Returns function oid definitions of the type, in a tuple with
        (eq, lt, le, ge, gt).  If not found, InvalidOid will be filled.

        """
        def btree_proc(opclass_oid, strategy):
            amopopr = self.pg_amop.find_amopopr(opclass_oid, strategy)
            proname = self.pg_operator.find_oprcode(amopopr)
            prosrc = self.pg_proc.find_prosrc_by_proname(proname)
            return prosrc

        typname = self.typid_sqltype(typid)
        # special case; regXXX is oid
        if typname.startswith('reg'):
            typid = self.sqltype_typid('oid')
        # and array's operator is found by anyarray.
        elif typname.startswith('_'):
            typid = self.sqltype_typid('anyarray')

        opclass_oid = self.pg_opclass.find_btree_oid_by_opcintype(typid)
        lt = btree_proc(opclass_oid, 1)
        le = btree_proc(opclass_oid, 2)
        eq = btree_proc(opclass_oid, 3)
        ge = btree_proc(opclass_oid, 4)
        gt = btree_proc(opclass_oid, 5)
        return (eq, lt, le, ge, gt)

class CatCoreGen(object):
    """The main class to generate catcore table from catdump."""

    def __init__(self, catdump):
        self._catdump = catdump

    def type_pointer(self, typid):
        pos = self._catdump.typid_pos(typid)
        typiddef = self._catdump.pg_type.oid_to_def(typid)
        return "&CatCoreTypes[{pos}] /* {typiddef} */".format(
                pos=pos, typiddef=typiddef)

    def index_pointer(self, relname, cols):
        pos = self._catdump.index_pos(relname, cols)
        if pos == -1:
            return "NULL"
        index = self._catdump.get_indexes_by_relname(relname)[pos]
        return "&{relname}Indexes[{pos}] /* {CamelCaseIndexId} */".format(
                relname=relname, pos=pos,
                CamelCaseIndexId=index['CamelCaseIndexId'])

    def generate_attribute(self, relname, col):
        typpointer = self.type_pointer(col['atttypid'])
        return ("""\t{{"{colname}", {attnum}, {typpointer}}}""".format(
                    colname=col['colname'],
                    attnum=col['attnum'],
                    typpointer=typpointer))

    def generate_index(self, relname, index):
        # we need copy as we'll modify it.
        index = index.copy()
        indexkeys = index['cols']

        attnums = ['InvalidAttrNumber'] * MAX_SCAN_NUM
        # convert attribute numbers to names.  The json doc has some weird
        # structure; element of index['cols'] is two-element array, with
        # attribute number in the first element and operator class in the
        # second element.
        for i, col in enumerate(indexkeys):
            attnum = self._catdump.attname_to_anum(relname, col[0])
            attnums[i] = attnum
        index['attnums'] = '{\n\t\t' + ',\n\t\t'.join(attnums) + '\n\t}'
        index['nkeys'] = len(indexkeys)

        # transfer data for just convenience
        if 'syscacheid' in index['with']:
            index['syscacheid'] = index['with']['syscacheid']
        else:
            index['syscacheid'] = '-1'

        index['is_unique'] = 'true' if index['unique'] == '1' else 'false'

        return ("""\t{{{CamelCaseIndexId}, {is_unique}, """
                """{attnums}, {nkeys}, """
                """{syscacheid}}}""".format(**index))

    def generate_fkey(self, relname, fkey):
        ref_table = fkey['pktable']
        ref_index = self.index_pointer(ref_table, fkey['pkcols'])

        attnums = ['InvalidAttrNumber'] * MAX_SCAN_NUM
        for i, fkcol in enumerate(fkey['fkcols']):
            attnums[i] = self._catdump.attname_to_anum(relname, fkcol)

        attnums = '{\n\t\t' + ',\n\t\t'.join(attnums) + '\n\t}'

        # ref_relname should be a pointer to relation, but because it's
        # a circular definition, we end up having a char pointer instead.
        return ("""\t{{{attnums}, {attnumlen}, """
                """"{ref_relname}", {ref_index}}}""".format(
                    ref_relname=ref_table,
                    ref_index=ref_index,
                    attnums=attnums,
                    attnumlen=len(fkey['fkcols'])
                    ))

    def generate_relation_attributes(self, relname):
        cols = self._catdump.get_cols_by_relname(relname)
        attrList = []
        for col in cols:
            coltext = self.generate_attribute(relname, col)
            attrList.append(coltext)

        return CatCoreTableRelationAttrTemplate.format(
                RelName=relname,
                AttrList=",\n".join(attrList)
                )

    def generate_relation_indexes(self, relname):
        indexes = self._catdump.get_indexes_by_relname(relname)
        indexList = []
        for index in indexes:
            indextext = self.generate_index(relname, index)
            indexList.append(indextext)

        return CatCoreTableRelationIndexTemplate.format(
                RelName=relname,
                IndexList=",\n".join(indexList)
                )

    def generate_relation_fkeys(self, relname):
        fkeys = self._catdump.get_fklist_by_relname(relname)
        fkeyList = []
        for fkey in fkeys:
            fkeytext = self.generate_fkey(relname, fkey)
            fkeyList.append(fkeytext)

        return CatCoreTableRelationFKeyTemplate.format(
                RelName=relname,
                FKeyList=",\n".join(fkeyList)
                )

    def generate_relation(self, relname):
        result = []
        catdump = self._catdump

        return ("""{{"{relname}", {CamelCaseRelationId}, """
                    """{relname}Attributes, {natts}, """
                    """{relname}Indexes, {nindexes}, """
                    """{relname}ForeignKeys, {nfkeys}, """
                    """{hasoid}}}""").format(
                    relname=relname,
                    CamelCaseRelationId=catdump.CamelCaseRelationId(relname),
                    natts=len(catdump.get_cols_by_relname(relname)),
                    nindexes=len(catdump.get_indexes_by_relname(relname)),
                    nfkeys=len(catdump.get_fklist_by_relname(relname)),
                    hasoid="true" if catdump.rel_has_oid(relname) else "false"
                    )

    def generate_type(self, typid):
        def fmgr_style(op):
            # e.g. 'int4eq' -> 'F_INT4EQ'.  See fmgroids.h
            if op is None:
                return "InvalidOid"
            return "F_" + op.upper()

        typid_def = self._catdump.pg_type.oid_to_def(typid)
        typtup = self._catdump.pg_type.findtup_by_typid(typid)
        typelem_def = self._catdump.pg_type.oid_to_def(typtup['typelem'])
        (eq, lt, le, ge, gt) = self._catdump.btree_ops(typid)
        return ("""{{{typid}, {typlen}, {typbyval}, '{typalign}', """
                """{typelem}, {eq}, {lt}, {le}, {ge}, {gt}}}""".format(
                    typid=typid_def,
                    typlen=typtup['typlen'],
                    typbyval='true' if typtup['typbyval'] == 't' else 'false',
                    typalign=typtup['typalign'],
                    typelem=typelem_def,
                    eq=fmgr_style(eq), lt=fmgr_style(lt), le=fmgr_style(le),
                    ge=fmgr_style(ge), gt=fmgr_style(gt)
                    ))

    def generate_table(self, outname):
        """Generates the whole table of C definitions and returns the string."""

        relnames = sorted(self._catdump.get_relnames())

        # generate attribute array.
        attributeList = []
        for relname in relnames:
            text = self.generate_relation_attributes(relname)
            attributeList.append(text)

        # special catcore attribute for oid column.
        oidTypePointer = self.type_pointer(self._catdump.sqltype_typid('oid'))

        # generate index array.
        indexList = []
        for relname in relnames:
            text = self.generate_relation_indexes(relname)
            indexList.append(text)

        # generate fkey array.
        fkeyList = []
        for relname in relnames:
            text = self.generate_relation_fkeys(relname)
            fkeyList.append(text)

        # generate relation array.
        relationList = []
        for relname in relnames:
            text = self.generate_relation(relname)
            relationList.append("\t" + text)

        # generate type array.
        typeList = []
        for typid in self._catdump.unique_typids_sorted():
            text = self.generate_type(typid)
            typeList.append("\t" + text)

        # write out.  The template is at the bottom of this file.
        with open(outname, "w") as out:
            body = CatCoreTableTemplate.format(
                        FileName=outname,
                        AttributeList="\n".join(attributeList),
                        OidTypePointer=oidTypePointer,
                        IndexList="\n".join(indexList),
                        FKeyList="\n".join(fkeyList),
                        RelationList=",\n".join(relationList),
                        RelationSize=len(relationList),
                        TypeList=",\n".join(typeList),
                        TypeSize=len(typeList),
                        ProgName=sys.argv[0],
                        GenerateTime=datetime.datetime.utcnow()
                        )
            out.write(body)

def main():
    if len(sys.argv) < 3:
        print("%s: OUTNAME JSONPATH" % sys.argv[0])
        sys.exit(1)
    outname = sys.argv[1]
    catdumppath = sys.argv[2]
    with open(catdumppath) as fp:
        catdump = CatDumpReader(json.load(fp))
        generator = CatCoreGen(catdump)
        generator.generate_table(outname)

### Templates start ###
CatCoreTableTemplate = """
/*-------------------------------------------------------------------------
 *
 * {FileName}
 *	  Auto-generated C file from catdump json document. Don't edit manually.
 *
 * WARNING: DO NOT MODIFY THIS FILE:
 * Generated by {ProgName}
 * {GenerateTime:%Y-%m-%d %H:%M:%S}
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/catcore.h"
#include "catalog/catalog.h"
#include "catalog/gp_configuration.h"
#include "catalog/gp_id.h"
#include "catalog/gp_master_mirroring.h"
#include "catalog/gp_policy.h"
#include "catalog/gp_san_config.h"
#include "catalog/gp_segment_config.h"
#include "catalog/gp_verification_history.h"
#include "catalog/gp_version.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_appendonly_alter_column.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_autovacuum.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filespace_entry.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_listener.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_rule.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_window.h"
#include "catalog/pg_tidycat.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

const CatCoreType CatCoreTypes[] = {{
{TypeList}
}};
const int CatCoreTypeSize = {TypeSize};

{AttributeList}

const CatCoreAttr TableOidAttr = {{
    "oid", ObjectIdAttributeNumber, {OidTypePointer}
}};

{IndexList}

{FKeyList}

const CatCoreRelation CatCoreRelations[] = {{
{RelationList}
}};
const int CatCoreRelationSize = {RelationSize};
""".lstrip()

CatCoreTableRelationAttrTemplate = """
const CatCoreAttr {RelName}Attributes[] = {{
{AttrList}
}};
""".lstrip()

CatCoreTableRelationIndexTemplate = """
const CatCoreIndex {RelName}Indexes[] = {{
{IndexList}
}};
""".lstrip()

CatCoreTableRelationFKeyTemplate = """
const CatCoreFKey {RelName}ForeignKeys[] = {{
{FKeyList}
}};
""".lstrip()
if __name__ == '__main__':
    main()

