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



def test_separators():
    import simplejson
    import textwrap
    
    h = [['blorpie'], ['whoops'], [], 'd-shtaeou', 'd-nthiouh', 'i-vhbjkhnth',
         {'nifty': 87}, {'field': 'yes', 'morefield': False} ]

    expect = textwrap.dedent("""\
    [
      [
        "blorpie"
      ] ,
      [
        "whoops"
      ] ,
      [] ,
      "d-shtaeou" ,
      "d-nthiouh" ,
      "i-vhbjkhnth" ,
      {
        "nifty" : 87
      } ,
      {
        "field" : "yes" ,
        "morefield" : false
      }
    ]""")


    d1 = simplejson.dumps(h)
    d2 = simplejson.dumps(h, indent=2, sort_keys=True, separators=(' ,', ' : '))

    h1 = simplejson.loads(d1)
    h2 = simplejson.loads(d2)

    assert h1 == h
    assert h2 == h
    assert d2 == expect
