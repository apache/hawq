# prevent included makefiles from stealing default target
default: all

subdir = src/bin/gpfdist
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# NO_M64 set in Makefile.global turns off default to 64-bit
# here, select bitness we desire this build for gcc and ld
CC=$(strip $(BLD_CC) $(BLD_CFLAGS))

GPFDISTDIR=$(CURDIR)
CDBINCL=-I$(GPFDISTDIR)/../../../src/include

ifeq "$(BLD_ARCH)" "win32"
override CC=$(strip /usr/local/mingw32/bin/mingw32-gcc $(BLD_CFLAGS))
override BLD_CFLAGS += -L/usr/local/mingw32/mingw32/lib -I/usr/local/mingw32/mingw32/include -DWIN32 $(CDBINCL)/port/win32 $(CDBINCL)/port/win32_msvc -DFRONTEND
GPFDISTFILES=gpfdist.c
GPFDISTFILES_EXTRA=fstream.c gfile.c transform.c glob.c $(CURDIR)/../src/port/dirent.c $(CURDIR)/../src/port/strlcpy.c $(CURDIR)/../src/port/win32error.c $(CURDIR)/../src/port/snprintf.c
GPFDISTOBJS_EXTRA=$(strip $(patsubst %.c,%.o,$(notdir $(GPFDISTFILES_EXTRA))))
GPFDIST_INCLUDES=-I$(BLD_TOP)/src/gpfdist/include
GPFDIST_LIBS=-L$(CURDIR)/../src/port -lpgport -lws2_32 -lgdi32
EXT_EXE=.exe
else
GPFDISTFILES=fstream.c gfile.c gpfdist.c transform.c
GPFDISTOBJSFILES=$(subst .c,.o,$(GPFDISTFILES))
# link with static libyaml so gpfdist won't need LD_LIBRARY_PATH
ifneq "$(findstring $(BLD_ARCH),aix5_ppc_64 aix5_ppc_32 osx104_x86)" ""
GPFDIST_LIBS=-lyaml -lz -lbz2
else
GPFDIST_LIBS=$(BLD_THIRDPARTY_LIB_DIR)/libyaml.a -lz -lbz2
endif
endif

export CC
export LDFLAGS=$(BLD_CFLAGS)

SHELL=bash
OPT=-g -O3
LIBEVENT=libevent-1.4.3-stable
OPENSSL=openssl-0.9.8r
OPENSSL_LIBS=-lssl -lcrypto
GP_VERSION:=$(shell cat $(BLD_TOP)/VERSION)

# flag to include/exclude gpfdist transformation support
GPFXDIST=-DGPFXDIST

INCLUDES=-I$(BLD_THIRDPARTY_INCLUDE_DIR) $$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --includes) -I. $(BLD_THIRDPARTY_OPENSSL_INCLUDES) 
CFLAGS=$(OPT) $$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --cflags) -Wall $(GPFXDIST)
APRLIBS=$$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --link-ld --libs) 
APULIBS= $$($(BLD_THIRDPARTY_BIN_DIR)/apu-1-config --link-ld --libs)
CPPFLAGS=$$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --cppflags) -DGP_VERSION="$(GP_VERSION)"
ifeq "$(BLD_ARCH)" "win32"
GPFDIST_CPPFLAGS=$$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --cppflags) -DGP_VERSION="$(GP_VERSION)"
GPFDIST_CPPFLAGS_EXTRA=$$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --cppflags)
else
sol10_sparc_32_CPPFLAGS=-D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
sol9_sparc_32_CPPFLAGS=-D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
sol8_sparc_32_CPPFLAGS=-D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
rhel4_x86_32_CPPFLAGS=-D_LARGEFILE_SOURCE=1 -D_FILE_OFFSET_BITS=64
rhel5_x86_32_CPPFLAGS=-D_LARGEFILE_SOURCE=1 -D_FILE_OFFSET_BITS=64
GPFDIST_CPPFLAGS=$$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --cppflags) -DGP_VERSION="$(GP_VERSION)" $($(BLD_ARCH)_CPPFLAGS)
endif

osx105_x86_GPFDIST_LDFLAGS=-Wl,-search_paths_first
GPFDIST_LDFLAGS=$($(BLD_ARCH)_GPFDIST_LDFLAGS)

all: 

#mkdir mkgpfdist

#mkgpma mkgp 

mkgpfdist: mkdir
	@echo --- gpfdist --------------------------------------------------
	cd src/gpfdist/ && \
	  ln -sf ../../../../../src/backend/utils/misc/fstream/*.c . && ls ../../../../../src/backend/utils/misc/fstream/*.c
	mkdir -p src/gpfdist/include
	perl -p -i -e 's,^prefix=.*$$,prefix="$(BLD_THIRDPARTY_DIR)",' $(BLD_THIRDPARTY_DIR)/bin/apr-1-config
ifeq "$(BLD_ARCH)" "win32"
	cd src/gpfdist/ && \
	  $(CC) $(INCLUDES) $(GPFDIST_INCLUDES) $(CDBINCL) $(GPFDIST_CPPFLAGS_EXTRA) $(CFLAGS) $(GPFDIST_LDFLAGS) -Werror -c \
	  $(GPFDISTFILES_EXTRA)
	cd src/gpfdist/ && \
	  $(CC) $(INCLUDES) $(GPFDIST_INCLUDES) $(CDBINCL) $(GPFDIST_CPPFLAGS) $(CFLAGS) $(GPFDIST_LDFLAGS) -Werror -o gpfdist$(EXT_EXE) \
	  $(GPFDISTFILES) $(GPFDISTOBJS_EXTRA) $(APRLIBS) -levent $(OPENSSL_LIBS) -L$(BLD_THIRDPARTY_LIB_DIR) $(GPFDIST_LIBS)
else
	for file in $(GPFDISTFILES); do \
	    (cd src/gpfdist/ && \
	      $(CC) $(INCLUDES) $(GPFDIST_INCLUDES) $(CDBINCL) $(GPFDIST_CPPFLAGS) $(CFLAGS) -Werror -c $${file}); \
	done
	cd src/gpfdist/ && \
	  $(CC) $(INCLUDES) $(GPFDIST_INCLUDES) $(CDBINCL) $(GPFDIST_CPPFLAGS) $(CFLAGS) $(GPFDIST_LDFLAGS) -Werror -o gpfdist$(EXT_EXE) \
	  $(GPFDISTOBJSFILES) $(GPFDISTOBJS_EXTRA) $(APRLIBS) -levent $(OPENSSL_LIBS) -L$(BLD_THIRDPARTY_LIB_DIR) $(GPFDIST_LIBS)
endif
	mkdir -p build/bin
	cp -f src/gpfdist/gpfdist$(EXT_EXE) build/bin/

mkdir:
	@mkdir -p src/build/lib src/build/include build/bin build/bin/lib

clean: 
	rm -f src/gpfdist/gpfdist
	rm -rf src/build
	find src -name '*.o' -exec rm -f {} \;

install:
	${INSTALL_SCRIPT} -d ${bindir}
	@for file in `find build/bin -type f`; \
		do \
			echo install $${file} into ${bindir};\
			${INSTALL_SCRIPT} $${file} ${bindir}; \
		done

