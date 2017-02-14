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

import sys, string, locale
locale.setlocale(locale.LC_ALL, "")

if len(sys.argv) <> 2:
   sys.stderr.write("Usage: sort.py filename\n")
   sys.exit(1)

infile = open(sys.argv[1], 'r')
list = infile.readlines()
infile.close()

for i in range(0, len(list)):
   list[i] = list[i][:-1] # chop!

list.sort(locale.strcoll)
print string.join(list, '\n')
