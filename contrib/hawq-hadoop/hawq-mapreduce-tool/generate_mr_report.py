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

############################################################################
# A script to generate pulse readable report.

infile = file('inputformat.logs')
outfile = file('inputformat.report','w')

testsuitename = ''
outline = ''
success = False

line = infile.readline()
while line:
    if line.find('Executing test case: ')!=-1:
        if len(outline) != 0:
            if success:
                outline = outline + "|Test Status|PASS"
            else:
                outline = outline + "|Test Status|FAILED"
            outfile.write(outline+'\n')

    	outline="Test Suite Name|" + testsuitename + "|Test Case Name|"
        success = False
        startIndex = line.find('Executing test case: ') + len('Executing test case: ')
    	endIndex=len(line)
        testcasename = line[startIndex:endIndex-1]
        outline = outline + testcasename + "|Test Detail|" + testcasename + " (0.00 ms)"

    elif line.find('Executing test suite: ')!=-1:
        startIndex = line.find('Executing test suite: ') + len('Executing test suite: ')
        endIndex = len(line)
        testsuitename = line[startIndex:endIndex-1]
 
    elif line.find('Successfully finish test case: ')!=-1:
        success = True
  
    line = infile.readline()

if len(outline) != 0:
    if success:
        outline = outline + "|Test Status|PASS"
    else:
        outline = outline + "|Test Status|FAILED"
    outfile.write(outline+'\n')

outfile.flush()
infile.close()
outfile.close()

