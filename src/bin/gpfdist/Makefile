
subdir = src/bin/gpfdist
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LIBS := -levent -lyaml -lz -lbz2 -lssl -lcrypto

code_dir     = $(CURDIR)/src/gpfdist/
GPFDISTFILES = fstream.c gfile.c gpfdist.c transform.c
OBJS         = $(subst .c,.o,$(GPFDISTFILES))

SHELL=bash
GP_VERSION:=$(shell cat $(top_builddir)/VERSION)

APR_CFG  := $(shell which apr-1-config)
APR_DIR  := $(shell dirname $(APR_CFG))/../
INCLUDES := $$($(APR_CFG) --includes) -I$(code_dir) -I$(CURDIR)/../../../src/include $(INCLUDES)
CFLAGS   := $$($(APR_CFG) --cflags) -Wall $(GPFXDIST) $(CFLAGS)
LIBS     := $$($(APR_CFG) --link-ld --libs) $(LIBS)

# flag to include/exclude gpfdist transformation support
GPFXDIST=-DGPFXDIST

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
	perl -p -i -e 's,^prefix=.*$$,prefix="$(APR_DIR)",' $(APR_CFG)

	for file in $(GPFDISTFILES); do \
	    ( $(CC) $(INCLUDES) $(CFLAGS) $(LIBS) -c $(code_dir)$${file}); \
	done
	# link
	$(CC) -o $(code_dir)gpfdist $(OBJS)  $(LIBS)

	mkdir -p build/bin
	mv -f src/gpfdist/gpfdist build/bin/

mkdir:
	@mkdir -p build/bin

clean: 
	rm -rf build
	find . -name '*.o' -exec rm -f {} \;

install:
	@for file in `find build/bin -type f`; \
		do \
			echo install $${file} into '$(DESTDIR)$(bindir)';\
			$(INSTALL_PROGRAM) $${file} '$(DESTDIR)$(bindir)'; \
		done

