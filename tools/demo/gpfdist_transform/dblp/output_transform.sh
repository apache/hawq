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
# output_transform.sh 
# sample output transformation demonstrating use of awk to convert
# tab separated output from GPDB back into simple xml
#
awk '
BEGIN {
    FS="\t"
    print "<?xml version=\"1.0\"?>"
    print "<dblp>"
}
{
    print "<" $2 "thesis key=\"" $1 "\">"
    print "<author>" $3 "</author>"
    print "<title>" $4 "</title>"
    print "<year>" $5 "</year>"
    print "<school>" $6 "</school>"
    print "</" $2 "thesis>"
    print ""
}
END {
    print "</dblp>"
}' > $*
