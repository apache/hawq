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

class SpecialFuncs(object):
    @classmethod
    def make_body(cls, func):
        key = 'make_body_' + func.funcname
        if key in cls.__dict__:
            return cls.__dict__[key].__get__(None, SpecialFuncs)(func)

    @staticmethod
    def make_body_MemoryContextAllocZeroImpl(func):
        return """
        void *p = malloc(size);
        memset(p, 0, size);
        return p;
        """

    @staticmethod
    def make_body_MemoryContextAllocImpl(func):
        return """
        void *p = malloc(size);
        return p;
        """

    @staticmethod
    def make_body_MemoryContextFreeImpl(func):
        return """
        free(pointer);
        """

    @staticmethod
    def make_body_MemoryContextStrdup(func):
        return """
        return strdup(string);
        """

    @staticmethod
    def make_body_MemoryContextReallocImpl(func):
        return """
        return realloc(pointer, size);
        """

    @staticmethod
    def make_body_MemoryContextAllocZeroAlignedImpl(func):
        return """
        void *p = malloc(size);
        memset(p, 0, size);
        return p;
        """
    
    @staticmethod
    def make_body_pnstrdup(func):
        return """
        char *out = malloc(len + 1);
        memcpy(out, in, len);
        out[len] = '\\0';
        return out;
        """

class ByValStructs(object):

    """These are structs over 32 bit and possibly passed by-value.
       As our mock framework doesn't accept 64 bit integer in some platform,
       we have to treat them specially.
    """
    type_names = set([
            'ArrayTuple',
            'CdbPathLocus',
            'Complex',
            'DbDirNode',
            'DirectDispatchCalculationInfo',
            'FileRepIdentifier_u',
            'FileRepOperationDescription_u',
            'FileRepRelFileNodeInfo_s',
            'FileRepVerifyArguments',
            'FileRepVerifyLogControl_s',
            'FileRepVerifyRequest_s',
            'instr_time',
            'Interval',
            'ItemPointerData',
            'NameData',
            'mpp_fd_set',
            'PGSemaphoreData',
            'PossibleValueSet',
            'PrimaryMirrorModeTransitionArguments',
            'RelFileNode',
            'struct timeval',
            'VariableStatData',
            'XLogRecPtr'
            ])
    @classmethod
    def has(cls, argtype):
        return argtype in cls.type_names
