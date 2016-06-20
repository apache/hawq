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

__all__ = ['Mark', 'YAMLError', 'MarkedYAMLError']

class Mark(object):

    def __init__(self, name, index, line, column, buffer, pointer):
        self.name = name
        self.index = index
        self.line = line
        self.column = column
        self.buffer = buffer
        self.pointer = pointer

    def get_snippet(self, indent=4, max_length=75):
        if self.buffer is None:
            return None
        head = ''
        start = self.pointer
        while start > 0 and self.buffer[start-1] not in u'\0\r\n\x85\u2028\u2029':
            start -= 1
            if self.pointer-start > max_length/2-1:
                head = ' ... '
                start += 5
                break
        tail = ''
        end = self.pointer
        while end < len(self.buffer) and self.buffer[end] not in u'\0\r\n\x85\u2028\u2029':
            end += 1
            if end-self.pointer > max_length/2-1:
                tail = ' ... '
                end -= 5
                break
        snippet = self.buffer[start:end].encode('utf-8')
        return ' '*indent + head + snippet + tail + '\n'  \
                + ' '*(indent+self.pointer-start+len(head)) + '^'

    def __str__(self):
        snippet = self.get_snippet()
        where = "  in \"%s\", line %d, column %d"   \
                % (self.name, self.line+1, self.column+1)
        if snippet is not None:
            where += ":\n"+snippet
        return where

class YAMLError(Exception):
    pass

class MarkedYAMLError(YAMLError):

    def __init__(self, context=None, context_mark=None,
            problem=None, problem_mark=None, note=None):
        self.context = context
        self.context_mark = context_mark
        self.problem = problem
        self.problem_mark = problem_mark
        self.note = note

    def __str__(self):
        lines = []
        if self.context is not None:
            lines.append(self.context)
        if self.context_mark is not None  \
            and (self.problem is None or self.problem_mark is None
                    or self.context_mark.name != self.problem_mark.name
                    or self.context_mark.line != self.problem_mark.line
                    or self.context_mark.column != self.problem_mark.column):
            lines.append(str(self.context_mark))
        if self.problem is not None:
            lines.append(self.problem)
        if self.problem_mark is not None:
            lines.append(str(self.problem_mark))
        if self.note is not None:
            lines.append(self.note)
        return '\n'.join(lines)

