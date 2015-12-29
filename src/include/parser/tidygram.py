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
# This is a simple little utility to check that kwlist.h is synchronized with gram.y
#

import re

# Read the keywords file into memory
keywords = {}
errcount = 0

kwfile   = open('kwlist.h')
lastword = None
kwx = re.compile("PG_KEYWORD\(([^, ]*), *([^, ]*), *([^, ]*) *\)")
for line in kwfile:
    m = kwx.match(line)
    if m:
        (word, token, kind) = m.groups()
        keywords[token] = [word, kind]
        if lastword and lastword > word:
            print "kwlist.h not in sorted order: %s > %s" % (lastword, word)
            errcount += 1
        lastword = word

# Read the grammar file and ensure agreement
#
#   All of the keyword lists are simple token lists with no grammar rules
#   This simplifies parsing considerably:
#     - Check if the line matches one of the four keyword categories
#     - Check if the line terminates the list with a semicolon
#     - If we are in one of the keyword lists each token is on its own line.
#     - Extra handling to strip out comments
#
gramfile = open('../../backend/parser/gram.y')
kind     = None
for line in gramfile:
    line = re.sub("/\*.*\*/","", line)
    line = line.strip(' \t|\n')
    if len(line) == 0:
        continue
    elif line.startswith("unreserved_keyword:"):
        kind = "UNRESERVED_KEYWORD"
    elif line.startswith("col_name_keyword:"):
        kind = "COL_NAME_KEYWORD"
    elif line.startswith("reserved_keyword:"):
        kind = "RESERVED_KEYWORD"
    elif line.startswith("func_name_keyword:"):
        kind = "TYPE_FUNC_NAME_KEYWORD"
    elif line.startswith(";"):
        kind = None
    elif kind is not None:
        word = line
        if word not in keywords:
            errcount += 1
            print "%s: missing != %s (kwlist.h, gram.y)" % (word, kind)
            continue
        elif keywords[word][1] != kind:
            errcount += 1
            print "%s: %s != %s (kwlist.h, gram.y)" \
                % (word, keywords[word][1], kind)
        del keywords[word]

# Report anything not found in the loop above
for word in keywords:
    print "%s: %s != missing (kwlist.h, gram.y)" % (word, keywords[word][1])
    errcount += 1

    
if errcount > 0:
    print "%d errors found" % errcount
    exit(1)
