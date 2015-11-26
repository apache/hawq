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
import simplejson as S

def test_encoding1():
    encoder = S.JSONEncoder(encoding='utf-8')
    u = u'\N{GREEK SMALL LETTER ALPHA}\N{GREEK CAPITAL LETTER OMEGA}'
    s = u.encode('utf-8')
    ju = encoder.encode(u)
    js = encoder.encode(s)
    assert ju == js
    
def test_encoding2():
    u = u'\N{GREEK SMALL LETTER ALPHA}\N{GREEK CAPITAL LETTER OMEGA}'
    s = u.encode('utf-8')
    ju = S.dumps(u, encoding='utf-8')
    js = S.dumps(s, encoding='utf-8')
    assert ju == js
