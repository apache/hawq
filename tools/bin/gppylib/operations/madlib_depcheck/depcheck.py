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
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Main Madpack installation executable.
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
import sys
import getpass
import re
import os
import glob
import traceback
import subprocess
import datetime
from time import strftime
import tempfile
import shutil
import yaml
from collections import defaultdict
from gppylib.operations.gpMigratorUtil import *

logger = get_default_logger()

# ----------------------------------------------------------------------

def run_sql(sql, portid, con_args):
    """
    @brief Wrapper function for ____run_sql_query
    """
    return ____run_sql_query(sql, True, portid, con_args)

# ----------------------------------------------------------------------

def get_signature_for_compare(schema, proname, rettype, argument):
    """
    @brief Get the signature of a UDF/UDA for comparison
    """
    signature = '{0} {1}.{2}({3})'.format(rettype.strip(), schema.strip(),
                                          proname.strip(), argument.strip())
    signature = re.sub('"', '', signature)
    return signature.lower()

# ----------------------------------------------------------------------

class UpgradeBase:
    """
    @brief Base class for handling the upgrade
    """
    def __init__(self, schema, portid, con_args):
        self._schema = schema.lower()
        self._portid = portid
        self._con_args = con_args
        self._schema_oid = None
        self._get_schema_oid()

    # ================================

    """
    @brief Wrapper function for run_sql
    """
    def _run_sql(self, sql):
        return run_sql(sql, self._portid, self._con_args)

    # ================================

    """
    @brief Get the oids of some objects from the catalog in the current version
    """
    def _get_schema_oid(self):
        self._schema_oid = self._run_sql("""
            SELECT oid FROM pg_namespace WHERE nspname = '{schema}'
            """.format(schema=self._schema))[0]['oid']

    # ================================

    def _get_function_info(self, oid):
        """
        @brief Get the function name, return type, and arguments given an oid
        @note The function can only handle the case that proallargtypes is null,
        refer to pg_catalog.pg_get_function_identity_argument and
        pg_catalog.pg_get_function_result in PG for a complete implementation, which are
        not supported by GP
        """
        row = self._run_sql("""
            SELECT
                max(proname) AS proname,
                max(rettype) AS rettype,
                array_to_string(
                    array_agg(argname || ' ' || argtype order by i), ',') AS argument
            FROM
            (
                SELECT
                    proname,
                    textin(regtypeout(prorettype::regtype)) AS rettype,
                    CASE array_upper(proargtypes,1) WHEN -1 THEN ''
                        ELSE textin(regtypeout(unnest(proargtypes)::regtype))
                    END AS argtype,
                    CASE WHEN proargnames IS NULL THEN ''
                        ELSE unnest(proargnames)
                    END AS argname,
                    CASE array_upper(proargtypes,1) WHEN -1 THEN 1
                        ELSE generate_series(0, array_upper(proargtypes, 1))
                    END AS i
                FROM
                    pg_proc AS p
                WHERE
                    oid = {oid}
            ) AS f
            """.format(oid=oid))
        return {"proname": row[0]['proname'],
                "rettype": row[0]['rettype'],
                "argument": row[0]['argument']}

# ----------------------------------------------------------------------

class ChangeHandler(UpgradeBase):
    """
    @brief This class reads changes from the configuration file and handles
    the dropping of objects
    """
    def __init__(self, schema, portid, con_args, maddir):
        UpgradeBase.__init__(self, schema, portid, con_args)
        self._opr_ind_svec = None
        self._get_opr_indepent_svec()
        self._maddir = maddir
        #self._mad_dbrev = mad_dbrev
        self._newmodule = None
        self._udt = None
        self._udf = None
        self._uda = None
        self._udc = None
        self._load()

    # ================================

    def _get_opr_indepent_svec(self):
        """
        @brief Get the User Defined Operators independent of svec in the current version
        """
        rows = self._run_sql("""
            SELECT
                oprname,
                tl.typname AS typ_left,
                nspl.nspname AS nsp_left,
                tr.typname AS typ_right,
                nspr.nspname AS nsp_right
            FROM
                pg_operator AS o,
                pg_type AS tl,
                pg_type AS tr,
                pg_namespace AS nspl,
                pg_namespace AS nspr
            WHERE
                oprnamespace = {schema_oid} AND
                (
                    oprleft <> '{schema_madlib}.svec'::regtype AND
                    oprright <> '{schema_madlib}.svec'::regtype AND
                    oprleft = tl.oid AND
                    oprright = tr.oid AND
                    tl.typnamespace = nspl.oid AND
                    tr.typnamespace = nspr.oid
                )
            """.format(schema_madlib=self._schema, schema_oid=self._schema_oid))
        self._opr_ind_svec = {}
        for row in rows:
            self._opr_ind_svec[row['oprname']] = row

    # ================================

    def _load_config_param(self, config_iterable):
        """
        Replace schema_madlib with the appropriate schema name and
        make all function names lower case to ensure ease of comparison.

        Args:
            @param config_iterable is an iterable of dictionaries, each with
                        key = object name (eg. function name) and value = details
                        for the object. The details for the object are assumed to
                        be in a dictionary with following keys:
                            rettype: Return type
                            argument: List of arguments

        Returns:
            A dictionary that lists all specific objects (functions, aggregates, etc)
            with object name as key and a list as value, where the list
            contains all the items present in

            another dictionary with objects details
            as the value.
        """
        _return_obj = defaultdict(list)
        if config_iterable is not None:
            for each_config in config_iterable:
                for obj_name, obj_details in each_config.iteritems():
                    rettype = obj_details['rettype'].lower().replace(
                                                'schema_madlib', self._schema)
                    if obj_details['argument'] is not None:
                        argument = obj_details['argument'].lower().replace(
                                                'schema_madlib', self._schema)
                        all_arguments = [each_arg.strip()
                                         for each_arg in argument.split(',')]
                    else:
                        all_arguments = []
                    _return_obj[obj_name].append(
                                    {'rettype': rettype,
                                     'argument': ','.join(all_arguments)})
        return _return_obj

    # ================================

    def _load(self):
        """
        @brief Load the configuration file
        """
        filename = os.path.join(self._maddir, 'changelist.yaml')

        config = yaml.load(open(filename))
        self._newmodule = config['new module'] if config['new module'] else {}
        self._udt = config['udt'] if config['udt'] else {}
        self._udc = config['udc'] if config['udc'] else {}
        self._udf = self._load_config_param(config['udf'])
        self._uda = self._load_config_param(config['uda'])

    # ================================

    @property
    def newmodule(self):
        return self._newmodule

    # ================================

    @property
    def udt(self):
        return self._udt

    # ================================

    @property
    def uda(self):
        return self._uda

    # ================================

    @property
    def udf(self):
        return self._udf

    # ================================

    @property
    def udc(self):
        return self._udc

    # ================================

    def get_udf_signature(self):
        """
        @brief Get the list of UDF signatures for comparison
        """
        res = defaultdict(bool)
        for udf in self._udf:
            for item in self._udf[udf]:
                signature = get_signature_for_compare(
                    self._schema, udf, item['rettype'], item['argument'])
                res[signature] = True
        return res

    # ================================

    def get_uda_signature(self):
        """
        @brief Get the list of UDA signatures for comparison
        """
        res = defaultdict(bool)
        for uda in self._uda:
            for item in self._uda[uda]:
                signature = get_signature_for_compare(
                    self._schema, uda, item['rettype'], item['argument'])
                res[signature] = True
        return res

 # ----------------------------------------------------------------------

class ViewDependency(UpgradeBase):
    """
    @brief This class detects the direct/recursive view dependencies on MADLib
    UDFs/UDAs defined in the current version
    """
    def __init__(self, schema, portid, con_args, ch):
        UpgradeBase.__init__(self, schema, portid, con_args)
        self._ch = ch
        self._view2proc = None
        self._view2view = None
        self._view2def = None
        self._detect_direct_view_dependency()
        self._detect_recursive_view_dependency()
        self._filter_recursive_view_dependency()

    # ================================

    """
    @brief  Detect direct view dependencies on MADLib UDFs/UDAs
    """
    def _detect_direct_view_dependency(self):
        rows = self._run_sql("""
            SELECT
                view, nsp.nspname AS schema, procname, procoid, proisagg
            FROM
                pg_namespace nsp,
                (
                    SELECT
                        c.relname AS view,
                        c.relnamespace AS namespace,
                        p.proname As procname,
                        p.oid AS procoid,
                        p.proisagg AS proisagg
                    FROM
                        pg_class AS c,
                        pg_rewrite AS rw,
                        pg_depend AS d,
                        pg_proc AS p
                    WHERE
                        c.oid = rw.ev_class AND
                        rw.oid = d.objid AND
                        d.classid = 'pg_rewrite'::regclass AND
                        d.refclassid = 'pg_proc'::regclass AND
                        d.refobjid = p.oid AND
                        p.pronamespace = {schema_madlib_oid}
                ) t1
            WHERE
                t1.namespace = nsp.oid
        """.format(schema_madlib_oid=self._schema_oid))

        self._view2proc = defaultdict(list)
        for row in rows:
            if row['procname'] in self._ch.udf.keys() + self._ch.uda.keys():
                key = (row['schema'], row['view'])
                self._view2proc[key].append(
                    (row['procname'], row['procoid'],
                        True if row['proisagg'] == 't' else False))

    # ================================

    """
    @brief  Detect recursive view dependencies (view on view)
    """
    def _detect_recursive_view_dependency(self):
        rows = self._run_sql("""
            SELECT
                nsp1.nspname AS depender_schema,
                depender,
                nsp2.nspname AS dependee_schema,
                dependee
            FROM
                pg_namespace AS nsp1,
                pg_namespace AS nsp2,
                (
                    SELECT
                        c.relname depender,
                        c.relnamespace AS depender_nsp,
                        c1.relname AS dependee,
                        c1.relnamespace AS dependee_nsp
                    FROM
                        pg_rewrite AS rw,
                        pg_depend AS d,
                        pg_class AS c,
                        pg_class AS c1
                    WHERE
                        rw.ev_class = c.oid AND
                        rw.oid = d.objid AND
                        d.classid = 'pg_rewrite'::regclass AND
                        d.refclassid = 'pg_class'::regclass AND
                        d.refobjid = c1.oid AND
                        c1.relkind = 'v' AND
                        c.relname <> c1.relname
                    GROUP BY
                        depender, depender_nsp, dependee, dependee_nsp
                ) t1
            WHERE
                t1.depender_nsp = nsp1.oid AND
                t1.dependee_nsp = nsp2.oid
        """)

        self._view2view = defaultdict(list)
        for row in rows:
            key = (row['depender_schema'], row['depender'])
            val = (row['dependee_schema'], row['dependee'])
            self._view2view[key].append(val)

    # ================================

    """
    @brief  Filter out recursive view dependencies which are independent of
    MADLib UDFs/UDAs
    """
    def _filter_recursive_view_dependency(self):
        # Get recursive dependee list
        dependeelist = []
        checklist = self._view2proc
        while True:
            dependeelist.extend(checklist.keys())
            new_checklist = defaultdict(bool)
            for depender in self._view2view.keys():
                for dependee in self._view2view[depender]:
                    if dependee in checklist:
                        new_checklist[depender] = True
                        break
            if len(new_checklist) == 0:
                break
            else:
                checklist = new_checklist

        # Filter recursive dependencies not related with MADLib UDF/UDAs
        fil_view2view = defaultdict(list)
        for depender in self._view2view:
            dependee = self._view2view[depender]
            dependee = [r for r in dependee if r in dependeelist]
            if len(dependee) > 0:
                fil_view2view[depender] = dependee

        self._view2view = fil_view2view

    # ================================

    """
    @brief  Build the dependency graph (depender-to-dependee adjacency list)
    """
    def _build_dependency_graph(self, hasProcDependency=False):
        der2dee = self._view2view.copy()
        for view in self._view2proc:
            if view not in self._view2view:
                der2dee[view] = []
            if hasProcDependency:
                der2dee[view].extend(self._view2proc[view])

        graph = der2dee.copy()
        for der in der2dee:
            for dee in der2dee[der]:
                if dee not in graph:
                    graph[dee] = []
        return graph

    # ================================

    """
    @brief Check dependencies
    """
    def has_dependency(self):
        return len(self._view2proc) > 0

    # ================================

    """
    @brief Get the depended UDF/UDA signatures for comparison
    """
    def get_depended_func_signature(self, aggregate=True):
        res = {}
        for procs in self._view2proc.values():
            for proc in procs:
                if proc[2] is aggregate and (self._schema, proc) not in res:
                    funcinfo = self._get_function_info(proc[1])
                    if aggregate:
                        funcinfo['argument'] = ','.join([s.strip() for s in funcinfo['argument'].split(',')])
                    else:
                        funcinfo['argument'] = (','.join([m.group(1).strip()
                            for m in re.finditer(r"[\"\w]+([^,]*)", funcinfo['argument'])
                            if m.group(1) != '']))
                    signature = get_signature_for_compare(self._schema, proc[0],
                                                          funcinfo['rettype'],
                                                          funcinfo['argument'])
                    res[signature] = True
        return res

    # ================================

    def get_proc_w_dependency(self, aggregate=True):
        res = []
        for procs in self._view2proc.values():
            for proc in procs:
                if proc[2] is aggregate and (self._schema, proc) not in res:
                    res.append((self._schema, proc))
        res.sort()
        return res

    # ================================

    def get_depended_uda(self):
        """
        @brief Get dependent UDAs
        """
        self.get_proc_w_dependency(aggregate=True)

    # ================================

    def get_depended_udf(self):
        """
        @brief Get dependent UDFs
        """
        self.get_proc_w_dependency(aggregate=False)

    # ================================

    def _node_to_str(self, node):
        if len(node) == 2:
            res = '%s.%s' % (node[0], node[1])
        else:
            res = '%s.%s' % (self._schema, node[0])
        return res

    # ================================

    def _nodes_to_str(self, nodes):
        return [self._node_to_str(i) for i in nodes]

    # ================================

    def get_dependency_graph_str(self):
        """
        @brief Get the dependency graph string for print
        """
        graph = self._build_dependency_graph(True)
        nodes = list(graph.keys())
        nodes.sort()
        res = ["Dependency Graph (Depender-Dependee Adjacency List):\n"]
        for node in nodes:
            depends = self._nodes_to_str(graph[node])
            if len(depends) > 0:
                res.append("{0} -> [{1}]".format(
                    self._node_to_str(node),
                    ','.join(depends)))
        return "\n\t\t\t\t".join(res)

# ----------------------------------------------------------------------

class TableDependency(UpgradeBase):
    """
    @brief This class detects the table dependencies on MADLib UDTs defined in the
    current version
    """
    def __init__(self, schema, portid, con_args, ch):
        UpgradeBase.__init__(self, schema, portid, con_args)
        self._ch = ch
        self._table2type = None
        self._detect_table_dependency()

    # ================================

    def _detect_table_dependency(self):
        """
        @brief Detect the table dependencies on MADLib UDTs
        """
        rows = self._run_sql("""
            SELECT
                nsp.nspname AS schema,
                relname AS relation,
                attname AS column,
                typname AS type
            FROM
                pg_attribute a,
                pg_class c,
                pg_type t,
                pg_namespace nsp
            WHERE
                t.typnamespace = {schema_madlib_oid}
                AND a.atttypid = t.oid
                AND c.oid = a.attrelid
                AND c.relnamespace = nsp.oid
                AND c.relkind = 'r'
            ORDER BY
                nsp.nspname, relname, attname, typname
            """.format(schema_madlib_oid=self._schema_oid))

        self._table2type = defaultdict(list)
        for row in rows:
            key = (row['schema'], row['relation'])
            if (row['type'] != 'svec' and row['type'] != 'bytea8' and
                    row['type'] in [i.keys()[0] for i in self._ch.udt]):
                self._table2type[key].append(
                    (row['column'], row['type']))

    # ================================

    def has_dependency(self):
        """
        @brief Check dependencies
        """
        return len(self._table2type) > 0

    # ================================

    def get_depended_udt(self):
        """
        @brief Get the list of depended UDTs
        """
        res = defaultdict(bool)
        for table in self._table2type:
            for (col, typ) in self._table2type[table]:
                if typ not in res:
                    res[typ] = True
        return res

    # ================================

    def get_dependency_str(self):
        """
        @brief Get the dependencies in string for print
        """
        res = ['Table Dependency (schema.table.column -> MADlib type):\n']
        for table in self._table2type:
            for (col, udt) in self._table2type[table]:
                res.append("{0}.{1}.{2} -> {3}".format(table[0], table[1], col,
                                                       udt))
        return "\n\t\t\t\t".join(res)

# ----------------------------------------------------------------------

# Required Python version
py_min_ver = [2,6]

# Check python version
if sys.version_info[:2] < py_min_ver:
    print "ERROR: python version too old (%s). You need %s or greater." \
          % ('.'.join(str(i) for i in sys.version_info[:3]), '.'.join(str(i) for i in py_min_ver))
    exit(1)

# The default path for this script and changelist file is hardcoded here
maddir = os.path.dirname(os.path.abspath(__file__))

# Import MADlib python modules
import argparse

# ----------------------------------------------------------------------

# Some read-only variables
this = os.path.basename(sys.argv[0])    # name of this script

# Global variables
portid = None       # Target port ID (eg: pg90, gp40)
dbconn = None       # DB Connection object
dbver = None        # DB version
con_args = {}       # DB connection arguments
verbose = None      # Verbose flag

# ----------------------------------------------------------------------

## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Create a temp dir
# @param dir temp directory path
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
def __make_dir(dir):
    if not os.path.isdir(dir):
        try:
            os.makedirs(dir)
        except:
            print "ERROR: can not create directory: %s. Check permissions." % dir
            exit(1)

# ----------------------------------------------------------------------

## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Runs a SQL query on the target platform DB
# using the default command-line utility.
# Very limited:
#   - no text output with "new line" characters allowed
# @param sql query text to execute
# @param show_error displays the SQL error msg
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
def __run_sql_query(sql, show_error):
    global portid
    global con_args
    return ____run_sql_query(sql, show_error, portid, con_args)

# ----------------------------------------------------------------------

def ____run_sql_query(sql, show_error, portid = portid, con_args = con_args):
    # psql
    if portid in ('greenplum', 'postgres', 'hawq'):

        # Define sqlcmd
        sqlcmd = 'psql'
        delimiter = '|'

        # Test the DB cmd line utility
        std, err = subprocess.Popen(['which', sqlcmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if std == '':
            logger.fatal("Command not found: %s" % sqlcmd)
            raise RuntimeError("Command not found: %s" % sqlcmd)

        # Run the query
        runcmd = [ sqlcmd,
                    '-h', con_args['host'].split(':')[0],
                    '-p', con_args['host'].split(':')[1],
                    '-d', con_args['database'],
                    '-U', con_args['user'],
                    '-F', delimiter,
                    '-Ac', "set CLIENT_MIN_MESSAGES=error; " + sql]
        runenv = os.environ
        runenv["PGPASSWORD"] = con_args['password']
        runenv["PGOPTIONS"] = '-c search_path=public'
        std, err = subprocess.Popen(runcmd, env=runenv, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if err:
            logger.fatal("SQL command failed: \nSQL: %s \n%s" % (sql, err))
            raise RuntimeError("SQL command failed: \nSQL: %s \n%s" % (sql, err))

        # Convert the delimited output into a dictionary
        results = [] # list of rows
        i = 0
        for line in std.splitlines():
            if i == 0:
                cols = [name for name in line.split(delimiter)]
            else:
                row = {} # dict of col_name:col_value pairs
                c = 0
                for val in line.split(delimiter):
                    row[cols[c]] = val
                    c += 1
                results.insert(i,row)
            i += 1
        # Drop the last line: "(X rows)"
        try:
            results.pop()
        except:
            pass

    return results

# ----------------------------------------------------------------------

## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Upgrade MADlib
# @param schema MADlib schema name
# @param dbrev DB-level MADlib version
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
def __db_upgrade(schema):
    #logger.info("Detecting dependencies...")

    #logger.info("Loading change list...")
    ch = ChangeHandler(schema, portid, con_args, maddir)

    #logger.info("Detecting table dependencies...")
    td = TableDependency(schema, portid, con_args, ch)

    #logger.info("Detecting view dependencies...")
    vd = ViewDependency(schema, portid, con_args, ch)

    has_dependency = False

    if td.has_dependency():
        logger.info("*"*50)
        logger.info("Following user tables are dependent on MADlib types:")
        logger.info(td.get_dependency_str() + "\n")
        logger.info("*"*50)
        cd_udt = [udt for udt in td.get_depended_udt()
                      if udt in [i.keys()[0] for i in ch.udt]]
        if len(cd_udt) > 0:
            has_dependency = True
            logger.info("""

                User has objects dependent on the following MADlib types
                which are to be dropped during migration!

                        {0}

                These objects need to be dropped before proceeding with
                migration. Please backup them if necessary.
                """.format('\n\t\t\t'.join(cd_udt)))

    if vd.has_dependency():
        logger.info("*"*50)
        logger.info("Following user views are dependent on MADlib objects:")
        logger.info(vd.get_dependency_graph_str() + "\n")
        logger.info("*"*50)

        c_udf = ch.get_udf_signature()
        d_udf = vd.get_depended_func_signature(False)
        cd_udf = [udf for udf in d_udf if udf in c_udf]
        if len(cd_udf) > 0:
            has_dependency = True
            logger.info("""

                User has objects dependent on the following MADlib functions
                which are to be dropped during migration!

                        {0}

                These objects need to be dropped before proceeding with
                migration. Please backup them if necessary.
                """.format('\n\t\t\t'.join(cd_udf)))

        c_uda = ch.get_uda_signature()
        d_uda = vd.get_depended_func_signature(True)
        cd_uda = [uda for uda in d_uda if uda in c_uda]
        if len(cd_uda) > 0:
            has_dependency = True
            logger.info("""

                User has objects dependent on the following MADlib aggregates
                which are to be dropped during migration!

                        {0}

                These objects need to be dropped before proceeding with
                migration. Please backup them if necessary.
                """.format('\n\t\t\t'.join(cd_uda)))

    return has_dependency

# ----------------------------------------------------------------------

def unescape(string):
    """
    Unescape separation characters in connection strings, i.e., remove first
    backslash from "\/", "\@", "\:", and "\\".
    """
    if string is None:
        return None
    else:
        return re.sub(r'\\(?P<char>[/@:\\])', '\g<char>', string)

# ----------------------------------------------------------------------

def parseConnectionStr(connectionStr):
    """
    @brief Parse connection strings of the form
           <tt>[username[/password]@][hostname][:port][/database]</tt>

    Separation characters (/@:) and the backslash (\) need to be escaped.
    @returns A tuple (username, password, hostname, port, database). Field not
             specified will be None.
    """
    match = re.search(
        r'((?P<user>([^/@:\\]|\\/|\\@|\\:|\\\\)+)' +
        r'(/(?P<password>([^/@:\\]|\\/|\\@|\\:|\\\\)*))?@)?' +
        r'(?P<host>([^/@:\\]|\\/|\\@|\\:|\\\\)+)?' +
        r'(:(?P<port>[0-9]+))?' +
        r'(/(?P<database>([^/@:\\]|\\/|\\@|\\:|\\\\)+))?', connectionStr)
    return (
        unescape(match.group('user')),
        unescape(match.group('password')),
        unescape(match.group('host')),
        match.group('port'),
        unescape(match.group('database')))

# ----------------------------------------------------------------------

## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Main
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
def depcheck_for_madlib(argv = ''):

    parser = argparse.ArgumentParser(
                description='MADlib package manager (' + '' + ')',
                argument_default=False,
                formatter_class=argparse.RawTextHelpFormatter,
                epilog="""Example:

  $ ./depcheck.py -c gpadmin@mdw:5432

  This will detect all the dependencies on MADlib objects in all the
  database running on server mdw:5432. Installer will try to login as
  GPADMIN and will prompt for password.
""")

    parser.add_argument(
        '-c', '--conn', metavar='CONNSTR', nargs=1, dest='connstr', default=None,
        help= "Connection string of the following syntax:\n"
            + "  [user[/password]@][host][:port]\n"
            + "If not provided default values will be derived for PostgerSQL and Greenplum:\n"
            + "- user: PGUSER or USER env variable or OS username\n"
            + "- pass: PGPASSWORD env variable or runtime prompt\n"
            + "- host: PGHOST env variable or 'localhost'\n"
            + "- port: PGPORT env variable or '5432'\n"
            )

    args = parser.parse_args(argv)
    schema = "madlib"

    ##
    # Parse DB Platform (== PortID) and compare with Ports.yml
    ##
    global portid
    global dbver

    portid = "postgres"

    ##
    # Parse CONNSTR (only if PLATFORM and DBAPI2 are defined)
    ##
    if portid:
        connStr = "" if args.connstr is None else args.connstr[0]
        (c_user, c_pass, c_host, c_port, c_db) = parseConnectionStr(connStr)

        # Find the default values for PG and GP
        if portid in ('postgres', 'greenplum', 'hawq'):
            if c_user is None:
                c_user = os.environ.get('PGUSER', getpass.getuser())
            if c_pass is None:
                c_pass = os.environ.get('PGPASSWORD', None)
            if c_host is None:
                c_host = os.environ.get('PGHOST', 'localhost')
            if c_port is None:
                c_port = os.environ.get('PGPORT', '5432')
            if c_db is None:
                c_db = os.environ.get('PGDATABASE', c_user)

        # Get password
        if c_pass is None:
            c_pass = getpass.getpass("Password for user %s: " % c_user)

        # Set connection variables
        global con_args
        con_args['host'] = c_host + ':' + c_port
        con_args['database'] = "template1"
        con_args['user'] = c_user
        con_args['password'] = c_pass
    else:
        con_args = None
        dbrev = None

    db_list = __run_sql_query("SELECT d.datname as Name FROM pg_catalog.pg_database d ORDER BY 1;", True)

    has_dependency = False
    try:
        for c_db in db_list:
            con_args['database'] = c_db['name']
            try:
                logger.info("Detecting the dependencies on MADlib in database " + c_db['name'] + " ... ")
                schema_exist = __run_sql_query("SELECT n.nspname AS Name FROM pg_catalog.pg_namespace n WHERE n.nspname = 'madlib';", False)
                if len(schema_exist) > 0:
                    if __db_upgrade(schema):
                        has_dependency = True
                    else:
                        logger.info("No dependency on MADlib objects in database " + c_db['name'] + " is detected.")
                else:
                    logger.info("No MADlib is installed and no need to check dependencies.")
            except:
                logger.info("No MADlib is installed and no need to check dependencies.")
                pass
    except Exception as e:
        #Uncomment the following lines when debugging
        #print "Exception: " + str(e)
        #print sys.exc_info()
        #traceback.print_tb(sys.exc_info()[2])
        logger.fatal("Dependency detection failed.")
        raise RuntimeError("Dependency detection failed.")

    if has_dependency:
        raise RuntimeError("User has objects that depend on MADlib objects to be dropped in the migration."
                " Please remove those dependencies before proceeding with the migration."
                " Make backups for these objects if necessary.")

# ----------------------------------------------------------------------

## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Start Here
## # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == "__main__":

    # Run main
    depcheck_for_madlib(sys.argv[1:])

