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

#-----------------------------------------------------------------------------
#
# caller.py
#
# Collect CaQL caller places and emit to stdout.  The output is used to
# know the CaQL code coverage, which you can see in caqltrack regression test.
# The basic flow is to build `.i` files from `.c` and grep `caql1` lines
# which don't look like definition, then tokenize the line to fields.
#-----------------------------------------------------------------------------
import os
import subprocess
import sys

def lex_line(line):
    """Tokenize the caql1 line."""

    # state constants
    INITIAL = 0  # initial state; before cql1
    IN_PAR = 1   # inside parenthesis; eg cql1(xxxx)
    IN_STR = 2   # inside string literal

    i = 0
    l = len(line)
    currbuf = []
    buffers = []
    state = INITIAL
    while i < l:
        if state == INITIAL:
            # don't need to bother by white spaces, given the cql() definition
            if line.startswith('cql1(', i):
                i += 5
                level = 0
                state = IN_PAR
                continue
            i += 1
        elif state == IN_PAR:
            c = line[i]
            if c == '"':
                i += 1
                state = IN_STR
                continue
            elif c == ' ':
                pass
            elif c == ',':
                buffers.append(''.join(currbuf))
                currbuf = []
            elif c == '(':
                level += 1
                currbuf.append(c)
            elif c == ')':
                if level == 0:
                    break
                level -= 1
                currbuf.append(c)
            else:
                currbuf.append(c)
            i += 1
        elif state == IN_STR:
            c = line[i]
            if c == '"':
                i += 1
                state = IN_PAR
                continue
            else:
                currbuf.append(c)
            i += 1
        else:
            raise Exception()
    return buffers

def main():
    # cdb-pg directory
    rootdir = os.path.join(os.path.dirname(__file__), '../../../..')
    # caql.files know which `.c` files should be processed.
    input_file = os.path.join(rootdir, 'src/backend/catalog/caql/caql.files')
    for line in open(input_file):
        line = line.strip()
        abspath = os.path.join(rootdir, 'src/backend/catalog/caql', line)
        dirname = os.path.dirname(abspath)
        dirname_fromtop = os.path.relpath(abspath, rootdir)
        filename = os.path.basename(abspath)
        filename_i = os.path.splitext(filename)[0] + '.i'
        # run C preprocessor
        ret = subprocess.call(['make', '-C', dirname, filename_i], stdout=subprocess.PIPE)
        if ret != 0:
            sys.stderr.write('make failed for ' + filename_i)
            continue
        for line_i in open(os.path.join(dirname, filename_i)):
            # avoid definitions and find lines with 'cql1'
            if ('#define' not in line_i and
                not line_i.startswith('cq_list') and
                'cql1' in line_i):
                # tokenize
                words = lex_line(line_i)
                # we drop the trailing parts as we don't need
                tup = words[0:3]
                tup.append(dirname_fromtop)
                sys.stdout.write('\t'.join(tup) + "\n")

        # clean up `.i` files
        os.unlink(os.path.join(dirname, filename_i))

if __name__ == '__main__':
    main()
