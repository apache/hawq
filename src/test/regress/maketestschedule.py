#!/usr/bin/env python
#
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

#
# Given a set of tests on the command line (comma separated), builds a schedule and emits it to stdout
#
# Special tokens:  
#
#
# Note: if you add a new test with dependencies, update with a new call to add_edge
#
# Note: we could force drop.sql to be run after all others?  But I think using known_good_schedule as input for doAll
#       is enough -- unless the user specifically requests it.
#
# todo: test more cases where things are run in parallel and talk about the same tables and whatnot
#                         (get more of these dtm crashes i've seen).
#                         (partition.sql locktest needs a filter on mppsessionid to work in parallel??)
#                         also: is my problem running in parallel because of 
# todo: write up documentation for tt_ options (on wiki)
# todo: write up documentation for adding a new unit test, etc.
#
# Future improvements to make (to this, and pg_regress calling in general)
#      * reduce the overhead of some other things (like only run upg-transform if needed, only transform input
#                   .source files if not done already)
#      * use this for regular running (need to incorporate architecture mapping from the perl version
#      * when doing in parallel, we could be even smarter about grouping by time by improving how we manage dependences
#            (that is, right now a test always runs in a group based on how many dependencies it has -- but
#                 we could move it further down in order to get more parallelism as long as it doesn't conflict
#                 with one of its dependents!)
#                The better solution would be to have pg_regress.c take max level
#                 of parallelism and have start the next test from the same line when done a test from the same
#                 line).  Unfortunately, this doesn't seem like how --max-connections option to pg_regress works.
#      * make pg_regress do the diffing of results in parallel as well
#      * support tt_last_failed and tt_last_failed_partial to load the last failed test and rerun them
#                   (failed vs failed_partial depends on whether you use failures written at the end
#                          or those written as we go (affects if you hit ctrl-c))
#
import collections
import os.path
import re
import subprocess
import sys

SQL_DIR = "sql"
SOURCE_DIR = "input"
EXPECTED_DIR = "expected"

ARCHITECTURE_UNKNOWN = "unknown"

#
# This function ships with the product in utils.py
#
def readAllLinesFromFile(fileName, stripLines=False, skipEmptyLines=False):
    """
    @param stripLines if true then line.strip() is called on each line read
    @param skipEmptyLines if true then empty lines are not returned.  Beware!  This will throw off your line counts
                             if you are relying on line counts
    """
    res = []
    f = open(fileName)
    try:
        for line in f:
            if stripLines:
                line = line.strip()

            if skipEmptyLines and len(line) == 0:
            # skip it!
                pass
            else:
                res.append(line)
    finally:
        f.close()
    return res

class EstimatedDurations:
    """
    Simple map of test to estimated duration for tests.  The actual numbers don't matter, just
       the relative values (used for test ordering).  So, when updating this
       you should replace all of the data values, if possible.
    """

    def __init__(self):
        self.__estimatedDurationPerTest = {}
        def addEstimatedDurations(*nameDurationTuples):
            for nd in nameDurationTuples:
                self.__estimatedDurationPerTest[nd[0]] = nd[1]

        addEstimatedDurations(
                ("boolean", 0.24), ("char", 0.25), ("name", 0.16), ("varchar", 0.24),
                ("text", 0.13), ("int2", 0.96), ("int4", 1.73), ("int8", 0.51), ("oid", 0.19),
                ("float4", 0.44), ("float8", 2.23), ("bit", 0.53), ("numeric", 4.28),
                ("strings", 1.64), ("numerology", 0.88), ("point", 0.16), ("lseg", 0.12),
                ("box", 0.15), ("path", 0.22), ("polygon", 0.20), ("circle", 0.14),
                ("date", 0.29), ("time", 0.18), ("timetz", 0.18), ("timestamp", 0.96),
                ("timestamptz", 0.98), ("interval", 0.28), ("abstime", 0.19), ("reltime", 0.17),
                ("tinterval", 0.15), ("inet", 0.32), ("comments", 0.02), ("oidjoins", 3.21),
                ("type_sanity", 0.61), ("opr_sanity", 3.84), ("geometry", 1.04), ("horology", 1.09),
                ("insert", 0.94), ("create_function_1", 0.11), ("create_type", 1.85), ("create_table", 1.68),
                ("create_function_2", 0.12), ("copy", 11.63), ("copyselect", 0.64), ("constraints", 2.24),
                ("create_misc", 7.33), ("create_aggregate", 0.37), ("create_operator", 0.07), ("create_index", 40.89),
                ("drop_if_exists", 0.28), ("vacuum", 3.61), ("create_view", 1.04), ("errors", 0.43),
                ("select", 1.71), ("select_into", 1.13), ("select_distinct", 0.20), ("select_distinct_on", 0.06),
                ("select_having", 0.40), ("subselect", 1.67), ("union", 0.21), ("case", 0.41),
                ("join", 8.86), ("indexjoin", 2.95), ("aggregates", 0.98), ("direct_dispatch", 2.29), ("partition_pruning_with_fn", 0.19),
                ("distributed_transactions", 7.57), ("random", 0.24), ("btree_index", 0.13), ("hash_index", 0.27),
                ("update", 0.68), ("delete", 0.25), ("namespace", 0.29), ("select_views", 0.02),
                ("portals", 1.32), ("portals_p2", 0.22), ("cluster", 2.04), ("dependency", 0.58),
                ("guc", 0.09), ("limit", 0.13), ("temp", 1.20), ("rangefuncs_cdb", 17.33),
                ("prepare", 0.98), ("without_oid", 1.53), ("conversion", 0.22), ("truncate", 1.25),
                ("sequence", 0.58), ("rowtypes", 1.87), ("gpdiffcheck", 0.39), ("exttab1", 12.88),
                ("resource_queue", 0.77), ("gptokencheck", 0.01), ("gpcopy", 5.64), ("sreh", 11.69),
                ("olap_setup", 1.49), ("olap_group", 6.79), ("olap_window", 4.17), ("olap_window_seq", 3.36),
                ("tpch500GB", 0.72), ("partition", 75.95), ("appendonly", 47.16), ("aocs", 42.76),
                ("gp_hashagg", 0.50), ("gp_dqa", 1.29), ("gpic", 28.78), ("gpic_bigtup", 7.08),
                ("filter", 0.47), ("gpctas", 2.18), ("gpdist", 6.81), ("matrix", 0.35),
                ("gpdtm_plpgsql", 52.83), ("notin", 1.21), ("toast", 0.95),
                ("gpparams", 1.10), ("upg2", 6.71), ("alter_distribution_policy", 44.76), ("ereport", 4.82),
                ("gp_numeric_agg", 2.81), ("foreign_data", 1.51), ("gp_toolkit", 0.06),
                ("column_compression", 5.5))

    def getEstimatedDuration(self, testName, defaultIfNotThere=1):
        if testName in self.__estimatedDurationPerTest:
            return self.__estimatedDurationPerTest[testName]
        return defaultIfNotThere

class DependencyEntry:
    """
    An entry in the SimpleDependencyGraph
    """

    def __init__(self, testName, isOrderingDependencyOnly):
        """
        @param isOrderingDependencyOnly see SimpleDependencyGraph.addDependency
        """
        self.__testName = testName
        self.__isOrderingDependencyOnly = isOrderingDependencyOnly

    def getTestName(self):
        return self.__testName

    def isOrderingDependencyOnly(self):
        return self.__isOrderingDependencyOnly

class SimpleDependencyGraph:
    """
    Simple dependency graph.  Use of this will probably lead to O(N^2) or worse behavior but
       I tried using pygraph and I couldn't get the output order that I wanted...
       if it gets slow then we'll have to address it then.

       Note that because we don't have a complex dependency graph (usually a large number of tests depend on
          a small number of tests) this is probably not an issue.
    """

    def __init__(self):

    # maps from test name to DependencyEntry objects
        self.__testToListOfDependencies = {}
        self.__testToMaxDependencyLength = {}

    def areDependentsSatisfied(self, test, doneTests, allTestsBeingRun):
        """
        @param test the name of the test to check
        @param doneTests a map of [doneTest->True] for completed tests
        @param allTestsBeingRun a map of [test->True] for all tests that will be run in this test run
        """
        for dependency in self.__testToListOfDependencies.get(test, []):
            if dependency.getTestName() not in doneTests:
                if dependency.isOrderingDependencyOnly():
                    if dependency.getTestName() in allTestsBeingRun:
                    # ordering-only dependency is being run but not done yet
                        return False
                else:
                    return False
        return True

    def getDependencyTestNames(self, test, testsBeingRunMap):
        return [d.getTestName() for d in self.__testToListOfDependencies.get(test, [])
                if not d.isOrderingDependencyOnly() or d.getTestName() in testsBeingRunMap]

    def addDependency(self, thisTest, dependsOnThisOne, isOrderingDependencyOnly=False):
        """
        @param isOrderingDependencyOnly if true then this dependency is only an ordering dependency -- that is,
           thistest must be run after dependsOnThisOne, but thisTest can also run in a test run
           where dependsOnThisOne is not involved! 
        """

        # clear stats
        self.__testToMaxDependencyLength = None

        # do the add of dependency
        if thisTest not in self.__testToListOfDependencies:
            self.__testToListOfDependencies[thisTest] = []

        for tEntry in self.__testToListOfDependencies[thisTest]:
            # yes, this assertion does not check isOrderingDependencyOnly -- caller should
            #   pick which they want (the stricter, False for isOrderingDependencyOnly)
            assert tEntry.getTestName() != dependsOnThisOne, \
                    "Duplicate entry for %s->%s dependency" % (thisTest, dependsOnThisOne)


        self.__testToListOfDependencies[thisTest].append( DependencyEntry(dependsOnThisOne, isOrderingDependencyOnly))

        # make sure the graph knows about the dependency as well
        if dependsOnThisOne not in self.__testToListOfDependencies:
            self.__testToListOfDependencies[dependsOnThisOne] = []

    def updateStats(self, testsBeingRunMap):
        """
        Update some stats info used by getStatXXX calls
        """
        self.__testToMaxDependencyLength = {}
        toProcess = collections.deque(self.__testToListOfDependencies.keys())

        safetyCheck = 0
        sentinel = 10000000
        while toProcess:
            test = toProcess.pop()
            maxCount = -1 # -1 to get one with no dependents to have value 0 after the increment
            for d in self.getDependencyTestNames(test, testsBeingRunMap):
                maxCount = max(maxCount, self.__testToMaxDependencyLength.get(d, sentinel))

            if maxCount == sentinel:
                toProcess.appendleft(test)
            else: self.__testToMaxDependencyLength[test] = maxCount + 1

            safetyCheck += 1
            if safetyCheck >= 10000000:
                raise Exception("Failed to update stats")

    def getStatMaxDependencyLength(self, test):
        assert self.__testToMaxDependencyLength is not None, "updateStats has not been called since last changes"

        return self.__testToMaxDependencyLength.get(test, 0)

class ArchitectureSpecificTemplateManager:
    """
    Deals with architecture-specific templates for tests (files in sql/input/expected directories that
          end with .tpl) that are used according to the current machine architecture 
    """

    def __init__(self):
        self.__architecture = self.__readArchitecture()

    def ensureTemplateWritten(self, testName):
        """
        Call to ensure that the template file(s) have been copied to the sql/output/etc. directories
          as needed.

        @return True if the sql/source file are there after this function runs

        """
        sqlFile = SQL_DIR + "/" + testName + ".sql"
        sourceFile = SOURCE_DIR + "/" + testName + ".source"

        if os.path.exists(sqlFile) or os.path.exists(sourceFile):
            #
            # note: this will create a problem if architecture changes (because you copied
            #       directories between machines, for example)
            # We could just relink in all cases when there is an input template file...
            #
            return True

        return self.__linkFile(SQL_DIR, testName, "sql") and \
               self.__linkFile(EXPECTED_DIR, testName, "out")

    def __linkFile(self, dir, testName, suffix):
        templateFileName = testName + "." + self.__architecture + ".tpl"
        templateFile = os.path.join(dir, templateFileName)

        targetFileName = testName + "." + suffix
        targetFile = os.path.join(dir, targetFileName)

        if os.path.exists(templateFile) and self.__architecture != ARCHITECTURE_UNKNOWN:
            if os.path.exists(targetFile):
                os.remove(targetFile)
            os.symlink(templateFileName, targetFile)
            return True
        else:
            return False

    def __readArchitecture(self):

    #
    # determine architecture by using configuration entries in arch_config and
    #  checking for entries provided by this file command
    #
    #
        process = subprocess.Popen(["file", os.getenv("GPHOME", ".") + "/bin/postgres"], stdout=subprocess.PIPE)
        process.wait()
        archString = process.communicate()[0].strip()

        index = archString.find(":")
        if index == -1:
            return ARCHITECTURE_UNKNOWN
        archLabel = archString[index+1:].strip()

        confLineMatcher = re.compile("([^ ]+)\s*:\s*'(.*)'\s*")
        for archLine in readAllLinesFromFile("arch_config", stripLines=True, skipEmptyLines=True):
            match = confLineMatcher.match(archLine)
            if match is not None:
                (arch, regExString) = match.group(1,2)
                if ( re.compile(regExString).match(archLabel) != -1):
                    return arch

        return ARCHITECTURE_UNKNOWN;

class TestScheduler:

    def __init__(self):
        pass

    def printSchedule(self,sourceSchedule, testsToRun, doAll, enableTimeBasedSorting, parallelTestLimit ):

        orderedTests = self.__orderTests(sourceSchedule, testsToRun, doAll)

        templateManager = ArchitectureSpecificTemplateManager()
        for i, testList in enumerate(orderedTests):
            newTestList = []
            for test in testList:
                if not templateManager.ensureTemplateWritten(test):
                    print >> sys.stderr, "WARNING: test %s will be skipped because the input files " \
                            "(sql, source, or architecture-specific template) does not exist" % test
                else: newTestList.append(test)
            orderedTests[i] = newTestList

        for testList in orderedTests:
            startIndex = 0
            if enableTimeBasedSorting:
                testList = self.__sortTestsByEstimatedTimeDesc(testList)
            while startIndex < len(testList):
                print "test: " + " ".join(testList[startIndex:startIndex+parallelTestLimit])
                startIndex += parallelTestLimit

    def __sortTestsByEstimatedTimeDesc(self, testNames):
        durations = EstimatedDurations()
        toSort = []
        for i, name in enumerate(testNames):
            toSort.append((name, durations.getEstimatedDuration(name), i))

        def compareByTime(left, right):
            res = cmp(left[1], right[1])
            if res == 0:
                res = cmp(left[2], right[2])
            assert res != 0
            return res

        toSort.sort(compareByTime, reverse=True)

        return [tuple[0] for tuple in toSort]

    def __buildDependencyGraph(self):
        graph = SimpleDependencyGraph()
        def addEdges(src, *dest):
            for d in dest:
                graph.addDependency(src, d )

        #
        # Add dependencies between tests
        #
        # edges are (test, prequisites)
        #
        addEdges("create_type", "create_function_1")
        addEdges("create_table", "create_type")
        addEdges("create_function_2", "create_table")
        addEdges("copy", "create_function_2")
        addEdges("create_misc", "copy")
        addEdges("create_index", "create_misc", "polygon", "circle")
        addEdges("alter_table","create_index")
        addEdges("create_view", "create_misc", "create_operator")
        addEdges("errors", "create_table")
        addEdges("select", "create_misc", "int8")
        addEdges("select_into", "create_misc")
        addEdges("select_distinct", "copy", "create_view", "select")
        addEdges("select_distinct_on", "copy", "create_view", "select")
        addEdges("subselect", "copy", "int4", "int8")
        addEdges("union", "char", "float8", "int4", "int8", "text", "varchar")
        addEdges("join", "copy", "float8", "int4")
        addEdges("aggregates", "copy", "create_aggregate", "int8", "int4")
        addEdges("random", "copy")
        addEdges("btree_index", "copy")
        addEdges("hash_index", "copy")
        addEdges("select_views", "create_view")
        addEdges("portals", "copy", "create_misc")
        addEdges("portals_p2", "copy", "create_misc")
        addEdges("limit", "copy")
        addEdges("rangefuncs_cdb", "int4")
        addEdges("prepare", "copy")
        addEdges("rowtypes", "copy")
        addEdges("resource_queue", "copy")
        addEdges("olap_group", "olap_setup")
        addEdges("olap_window", "olap_setup")
        addEdges("olap_window_seq", "olap_setup")
        addEdges("arrays", "create_table", "copy")
        addEdges("transactions", "create_misc")

        # these both use (and pkill) gpdist, so keep them separate.
        graph.addDependency("sreh", "exttab1", isOrderingDependencyOnly=True)

        # upg2 changes some tables in ways opr_sanity does not like; todo: fix the test instead?
        graph.addDependency("upg2", "opr_sanity", isOrderingDependencyOnly=True)

        return graph


    def __orderTests(self, knownGoodScheduleFile, testsToRun, doAll):
        """
        Produce an ordering of the given testsToRun, also including any needed prerequisite tests
        """

        testsToRun = [t for t in testsToRun] # copy so we can modify later

        # parse test lines listed
        testLines = [line for line in readAllLinesFromFile(knownGoodScheduleFile, True, True) if line[0] != '#']
        testNameToCurrentAction = {}
        for line in testLines:
            if len(line.split(":")) != 2:
                raise Exception("Invalid line in %s: %s " % (knownGoodScheduleFile, line))
            (action, name) = line.split(":")
            name = name.strip()
            action = action.strip()

            testNameToCurrentAction[name] = action

            if doAll and action == "test":
                testsToRun.append(name)

        for test in testsToRun:
            testNameToCurrentAction[name] = "test"

        # build graph and add known dependencies
        graph = self.__buildDependencyGraph()

        #
        # enable testsToRun to include all dependencies
        #
        addedTests = {}
        for test in testsToRun:
            addedTests[test] = True
        doLoop = True # we must loop multiple times because testsBeingRunMap may change and so more ordering dependencies may appear
        while(doLoop):
            testsBeingRunMap = dict.fromkeys(testsToRun, True)
            doLoop = False
            testIndex = 0
            while testIndex < len(testsToRun):  # we modify testsToRun so do it this way
                for d in graph.getDependencyTestNames(testsToRun[testIndex], testsBeingRunMap):
                    if d not in addedTests:
                        addedTests[d] = True
                        testsToRun.append(d)
                        doLoop = True
                testIndex += 1

        assert(len(testsBeingRunMap) == len(testsToRun))

        #
        # now determine the final schedule, kind of N^2 but okay in practice, I think :)
        #
        graph.updateStats(testsBeingRunMap)

        addedTests = {}
        result = []
        while len(addedTests) != len(testsToRun):
            startLen = len(addedTests)
            haveUnaddedTest = False
            for test in testsToRun:
                if test in addedTests:
                    continue

                if graph.areDependentsSatisfied(test, addedTests, testsBeingRunMap):
                    index = graph.getStatMaxDependencyLength(test)

                    while index >= len(result):
                        result.append([])
                    result[index].append(test)
                    addedTests[test] = True

            if startLen == len(addedTests):
                raise Exception("Cannot resolve test order: %s" % (testsToRun))

        return result

def makeSpecifiedScheduleMain(argv=None):
    if argv is None:
        argv = sys.argv

    enableTimeBasedSorting = False # note: if enabled, this causes errors in the product for me
    parallelTestLimit = 1
    doAll = True
    testsToRun = []
    for arg in argv[1:]:
        for test in arg.split(","):
            test = test.strip()
            if test == "":
                pass
            elif test == "tt_parallel":
                parallelTestLimit = 4 # default parallel limit
            elif test.startswith("tt_parallel="):
                (dummy, parallelTestLimit) = test.split("=")
                parallelTestLimit = int(parallelTestLimit)
                assert parallelTestLimit >= 1
            elif test == "tt_sort_duration_desc":
                enableTimeBasedSorting = True
            else:
                doAll = False
                testsToRun.append(test)

    TestScheduler().printSchedule("known_good_schedule", testsToRun, doAll, enableTimeBasedSorting, parallelTestLimit)

if __name__ == '__main__':
    makeSpecifiedScheduleMain()
