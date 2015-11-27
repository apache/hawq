#!/bin/bash
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
# input_transform.sh 
# sample input transformation, demonstrating use of Java and Joost STX
# to convert MeF XML into escaped text suitable for loading into GPDB.
#
# java arguments:
#   -jar data/joost.jar       joost STX engine
#   -nodecl                   don't generate an <?xml?> declaration
#   $1 (or -)                 filename to process (or stdin)
#   mef/input_transform.stx   STX transformation
#
# This first step extracts interesting attributes from the input document
# (the "id" column in this example).
#
java \
    -jar data/joost.jar \
    -nodecl \
    $1 \
    mef/input_transform.stx | \
awk 'NF>0 {printf "%s|", $0}'

# This second awk command escapes newlines in the input document before
# sending it to GPDB so that the entire document ends up in our xml 'doc' column.
#
awk '{ printf "%s\\n",$0 }' $1
