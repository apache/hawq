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

class Token(object):
    def __init__(self, start_mark, end_mark):
        self.start_mark = start_mark
        self.end_mark = end_mark
    def __repr__(self):
        attributes = [key for key in self.__dict__
                if not key.endswith('_mark')]
        attributes.sort()
        arguments = ', '.join(['%s=%r' % (key, getattr(self, key))
                for key in attributes])
        return '%s(%s)' % (self.__class__.__name__, arguments)

#class BOMToken(Token):
#    id = '<byte order mark>'

class DirectiveToken(Token):
    id = '<directive>'
    def __init__(self, name, value, start_mark, end_mark):
        self.name = name
        self.value = value
        self.start_mark = start_mark
        self.end_mark = end_mark

class DocumentStartToken(Token):
    id = '<document start>'

class DocumentEndToken(Token):
    id = '<document end>'

class StreamStartToken(Token):
    id = '<stream start>'
    def __init__(self, start_mark=None, end_mark=None,
            encoding=None):
        self.start_mark = start_mark
        self.end_mark = end_mark
        self.encoding = encoding

class StreamEndToken(Token):
    id = '<stream end>'

class BlockSequenceStartToken(Token):
    id = '<block sequence start>'

class BlockMappingStartToken(Token):
    id = '<block mapping start>'

class BlockEndToken(Token):
    id = '<block end>'

class FlowSequenceStartToken(Token):
    id = '['

class FlowMappingStartToken(Token):
    id = '{'

class FlowSequenceEndToken(Token):
    id = ']'

class FlowMappingEndToken(Token):
    id = '}'

class KeyToken(Token):
    id = '?'

class ValueToken(Token):
    id = ':'

class BlockEntryToken(Token):
    id = '-'

class FlowEntryToken(Token):
    id = ','

class AliasToken(Token):
    id = '<alias>'
    def __init__(self, value, start_mark, end_mark):
        self.value = value
        self.start_mark = start_mark
        self.end_mark = end_mark

class AnchorToken(Token):
    id = '<anchor>'
    def __init__(self, value, start_mark, end_mark):
        self.value = value
        self.start_mark = start_mark
        self.end_mark = end_mark

class TagToken(Token):
    id = '<tag>'
    def __init__(self, value, start_mark, end_mark):
        self.value = value
        self.start_mark = start_mark
        self.end_mark = end_mark

class ScalarToken(Token):
    id = '<scalar>'
    def __init__(self, value, plain, start_mark, end_mark, style=None):
        self.value = value
        self.plain = plain
        self.start_mark = start_mark
        self.end_mark = end_mark
        self.style = style

