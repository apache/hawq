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
#

import subprocess
import sys
import re
orc_file_path = sys.argv[2]
p = subprocess.Popen(['orc-statistics',orc_file_path], stdout=subprocess.PIPE).communicate()[0].split("\n")

# get col name

metadata  = subprocess.Popen(['orc-metadata',orc_file_path], stdout=subprocess.PIPE).communicate()[0]

span_tuple = re.search('struct<.*>',metadata.replace('\n',''),0).span()

col_str  = metadata.replace('\n','')[span_tuple[0] + 7:span_tuple[1] - 1]

col_list = col_str.split(',')

col_name =[]

for case in col_list:
    if ':' in case:
        col_name.append(case.split(':')[0])

l = len(p)
str_ans = ''
stripe_num = -1
for i in range(0,l):
    cas = p[i]
    if 'Stripe'in cas:
        stripe_num += 1
    if i == l-1:
        str_ans += '}}}'
    if i == 0:
        str_ans += '{'
    if 'File' == cas[0:4] or cas =='':
        continue;
    if '***' in cas:
        if 'Column 0' not in cas or 'Stripe'  in cas:
            str_ans += '},' + cas.replace('***','"') + ':' + '{'
        else:
            str_ans += cas.replace('***','"') + ':' + '{'
    if '---' in cas:
        if 'Column 0' not in cas:
            str_ans += '},' + cas.replace('---','"') + ':' + '{'
        else:
            str_ans += cas.replace('---','"') + ':' + '{'
    if ':' in cas:
        tmp_str = cas.split(': ')
        for i in range(2,len(tmp_str)):
            tmp_str[1] += ':' + tmp_str[i]
        str_ans += '"' + tmp_str[0] + '":'
        if tmp_str[1].strip().isdigit() or cas.replace('.','',1).isdigit():
            str_ans += tmp_str[1]
        elif tmp_str[1].strip()=='no' :
            str_ans += 'false'
        elif tmp_str[1].strip()=='yes' :
            str_ans += 'true'
        else:
            str_ans += '"' + tmp_str[1] + '"'
        str_ans +=','
col_l = len(col_name)

for i in range(0,col_l):
    str_ans = str_ans.replace(' Column ' + str(i+1) + ' ',col_name[i])

if stripe_num == 0:
    str_ans = str_ans.replace(',}}}',"}}]}")
else:
    str_ans = str_ans.replace(',}}}',"}}}]}")
str_ans = str_ans.replace('" Stripe 0 ":','"Stripes":[')
for i in range(1,2):
    str_ans = str_ans.replace('" Stripe {} "'.format(i),'}},{{"Stripe {}"'.format(i))
for i in range(2,stripe_num+1):
    str_ans = str_ans.replace('" Stripe {} "'.format(i),'}}}},{{"Stripe {}"'.format(i))
file_path = '"File_path":"' + sys.argv[2] + '",'
pre_json = str_ans[0] + file_path + str_ans[1:len(str_ans)]
print(sys.argv[1] + '|' + pre_json.replace(',}','}'))
