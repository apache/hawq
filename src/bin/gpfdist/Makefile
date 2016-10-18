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

subdir = src/bin/gpfdist
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LIBS := -levent -lyaml -lz -lbz2 -lssl -lcrypto
ifeq ($(BUILD_TYPE), gcov)
LIBS   := -lgcov $(LIBS)
CFLAGS := -fprofile-arcs -ftest-coverage $(CFLAGS)
endif

code_dir     = $(CURDIR)/src/gpfdist/
GPFDISTFILES = fstream.c gfile.c gpfdist.c transform.c
OBJS         = $(subst .c,.o,$(GPFDISTFILES))

SHELL=bash
GP_VERSION:=$(shell cat $(top_builddir)/VERSION)
APR_CFG  := $(shell which apr-1-config)
APR_DIR  := $(shell dirname $(APR_CFG))/../

# flag to include/exclude gpfdist transformation support
GPFXDIST=-DGPFXDIST

INCLUDES := $$($(APR_CFG) --includes) -I$(code_dir) -I$(CURDIR)/../../../src/include $(INCLUDES)
CFLAGS   := $$($(APR_CFG) --cflags) -Wall $(GPFXDIST) $(CFLAGS)
LIBS     := $$($(APR_CFG) --link-ld --libs) $(LIBS)

# workaround for 'brew link openssl --force' error on MacOS 10.12
UNAME = $(shell uname)
ifeq (Darwin, $(UNAME))
	INCLUDES := $(INCLUDES) -I/usr/local/opt/openssl/include
	LIBS := $(LIBS) -L/usr/local/opt/openssl/lib
endif

sol10_sparc_32_CPPFLAGS = -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
sol9_sparc_32_CPPFLAGS  = -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
sol8_sparc_32_CPPFLAGS  = -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
rhel4_x86_32_CPPFLAGS   = -D_LARGEFILE_SOURCE=1 -D_FILE_OFFSET_BITS=64
rhel5_x86_32_CPPFLAGS   = -D_LARGEFILE_SOURCE=1 -D_FILE_OFFSET_BITS=64
GPFDIST_CPPFLAGS        = $$($(APR_CFG) --cppflags) -DGP_VERSION="$(GP_VERSION)" $($(BLD_ARCH)_CPPFLAGS)
CFLAGS:=$(GPFDIST_CPPFLAGS) $(CFLAGS)

all: mkdir mkgpfdist

mkgpfdist: mkdir
	@echo --- gpfdist --------------------------------------------------
	cd src/gpfdist/ && \
	  ln -sf ../../../../../src/backend/utils/misc/fstream/*.c . && ls ../../../../../src/backend/utils/misc/fstream/*.c
	#perl -p -i -e 's,^prefix=.*$$,prefix="$(APR_DIR)",' $(APR_CFG)

	for file in $(GPFDISTFILES); do \
	    ( $(CC) $(INCLUDES) $(CFLAGS) $(LIBS) -c $(code_dir)$${file}); \
	    if [[ $$? -ne 0 ]]; then exit 1; fi \
	done
	# link
	$(CC) $(CFLAGS) -o $(code_dir)gpfdist $(OBJS)  $(LIBS)

	mkdir -p build/bin
	mv -f src/gpfdist/gpfdist build/bin/

mkdir:
	@mkdir -p build/bin

clean: 
	rm -rf build
	find . -name '*.o' -exec rm -f {} \;

install: all
	@for file in `find build/bin -type f`; \
		do \
			echo install $${file} into '$(DESTDIR)$(bindir)';\
			$(INSTALL_PROGRAM) $${file} '$(DESTDIR)$(bindir)'; \
		done

