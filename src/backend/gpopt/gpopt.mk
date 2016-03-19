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

GREP_SED_VAR = $(top_builddir)/src/backend/gpopt/ivy.xml | sed -e 's|\(.*\)rev="\(.*\)"[ ]*conf\(.*\)|\2|'

XERCES_VER  = $(shell grep "\"xerces-c\""   $(GREP_SED_VAR))
LIBGPOS_VER = $(shell grep "\"libgpos\""    $(GREP_SED_VAR))
OPTIMIZER_VER = $(shell grep "\"optimizer\""  $(GREP_SED_VAR))

BLD_FLAGS = $(ARCH_FLAGS) -D$(ARCH_BIT) -D$(ARCH_CPU) -D$(ARCH_OS) $(GPOPT_flags)
override CPPFLAGS := -fPIC $(CPPFLAGS)
override CPPFLAGS := $(BLD_FLAGS)  $(CPPFLAGS)
override CPPFLAGS := -DGPOS_VERSION=\"$(LIBGPOS_VER)\" $(CPPFLAGS)
override CPPFLAGS := -DGPOPT_VERSION=\"$(OPTIMIZER_VER)\" $(CPPFLAGS)
override CPPFLAGS := -DXERCES_VERSION=\"$(XERCES_VER)\" $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libgpos/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libgpdbcost/include $(CPPFLAGS)
