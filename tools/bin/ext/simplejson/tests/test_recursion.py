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
import simplejson

def test_listrecursion():
    x = []
    x.append(x)
    try:
        simplejson.dumps(x)
    except ValueError:
        pass
    else:
        assert False, "didn't raise ValueError on list recursion"
    x = []
    y = [x]
    x.append(y)
    try:
        simplejson.dumps(x)
    except ValueError:
        pass
    else:
        assert False, "didn't raise ValueError on alternating list recursion"
    y = []
    x = [y, y]
    # ensure that the marker is cleared
    simplejson.dumps(x)

def test_dictrecursion():
    x = {}
    x["test"] = x
    try:
        simplejson.dumps(x)
    except ValueError:
        pass
    else:
        assert False, "didn't raise ValueError on dict recursion"
    x = {}
    y = {"a": x, "b": x}
    # ensure that the marker is cleared
    simplejson.dumps(x)

class TestObject:
    pass

class RecursiveJSONEncoder(simplejson.JSONEncoder):
    recurse = False
    def default(self, o):
        if o is TestObject:
            if self.recurse:
                return [TestObject]
            else:
                return 'TestObject'
        simplejson.JSONEncoder.default(o)

def test_defaultrecursion():
    enc = RecursiveJSONEncoder()
    assert enc.encode(TestObject) == '"TestObject"'
    enc.recurse = True
    try:
        enc.encode(TestObject)
    except ValueError:
        pass
    else:
        assert False, "didn't raise ValueError on default recursion"
