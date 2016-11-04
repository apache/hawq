##-------------------------------------------------------------------------------------
##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
## @doc: GP optimizer build 
##
## @author: solimm1
##
##-------------------------------------------------------------------------------------

UNAME = $(shell uname)
UNAME_P = $(shell uname -p)
UNAME_M = $(shell uname -m)
ARCH_OS = GPOS_$(UNAME)
ARCH_CPU = GPOS_$(UNAME_P)

ifeq (Darwin, $(UNAME))
        LDSFX = dylib
else
        LDSFX = so
endif

BLD_TYPE=opt

GPOPT_flags = -g3 -DGPOS_DEBUG
ifeq "$(BLD_TYPE)" "opt"
	GPOPT_flags = -O3 -fno-omit-frame-pointer -g3
endif

ifeq (x86_64, $(UNAME_M))
	ARCH_BIT = GPOS_64BIT
else
	ARCH_BIT = GPOS_32BIT
endif

ifeq ($(ARCH_BIT), GPOS_32BIT)
	ARCH_FLAGS = -m32
else
	ARCH_FLAGS = -m64
endif

ORCA_BLD_PATH=$(abs_top_builddir)/depends/thirdparty

LIBGPOS_DIR = $(ORCA_BLD_PATH)/gpos/build/install/$(prefix)
LIBXERCES_DIR = $(ORCA_BLD_PATH)/gp-xerces/build/install/$(prefix)
LIBNAUCRATES_DIR = $(ORCA_BLD_PATH)/gporca/build/install/$(prefix)
LIBGPDBCOST_DIR = $(ORCA_BLD_PATH)/gporca/build/install/$(prefix)
LIBGPOPT_DIR = $(abs_top_builddir)/src/backend/gpopt/build/install/$(prefix)

BLD_FLAGS = $(ARCH_FLAGS) -D$(ARCH_BIT) -D$(ARCH_CPU) -D$(ARCH_OS) $(GPOPT_flags)
override CPPFLAGS := -fPIC $(CPPFLAGS)
override CPPFLAGS := $(BLD_FLAGS)  $(CPPFLAGS)
override CPPFLAGS := -I $(LIBGPOS_DIR)/include $(CPPFLAGS)
override CPPFLAGS := -I $(LIBXERCES_DIR)/include $(CPPFLAGS)
override CPPFLAGS := -I $(LIBGPOPT_DIR)/include $(CPPFLAGS)
override CPPFLAGS := -I $(LIBNAUCRATES_DIR)/include $(CPPFLAGS)
override CPPFLAGS := -I $(LIBGPDBCOST_DIR)/include $(CPPFLAGS)
