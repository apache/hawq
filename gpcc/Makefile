# prevent included makefiles from stealing default target
default: all

BLD_TOP=$(shell dirname `pwd`)
include $(BLD_TOP)/Makefile.global
include $(BLD_TOP)/Makefile.thirdparty

# NO_M64 set in Makefile.global turns off default to 64-bit
# here, select bitness we desire this build for gcc and ld
CC=$(strip $(BLD_CC) $(BLD_CFLAGS))

GPCC=$(CURDIR)
CDBINCL=-I$(GPCC)/../cdb-pg/src/include

ifeq "$(BLD_ARCH)" "win32"
override CC=$(strip /usr/local/mingw32/bin/mingw32-gcc $(BLD_CFLAGS))
override BLD_CFLAGS += -L/usr/local/mingw32/mingw32/lib -I/usr/local/mingw32/mingw32/include -DWIN32 $(CDBINCL)/port/win32 $(CDBINCL)/port/win32_msvc -DFRONTEND
GPFDISTFILES=gpfdist.c
GPFDISTFILES_EXTRA=fstream.c gfile.c transform.c glob.c $(CURDIR)/../cdb-pg/src/port/dirent.c $(CURDIR)/../cdb-pg/src/port/strlcpy.c $(CURDIR)/../cdb-pg/src/port/win32error.c $(CURDIR)/../cdb-pg/src/port/snprintf.c
GPFDISTOBJS_EXTRA=$(strip $(patsubst %.c,%.o,$(notdir $(GPFDISTFILES_EXTRA))))
GPFDIST_INCLUDES=-I$(GPCC)/src/gpfdist/include
GPFDIST_LIBS=-L$(CURDIR)/../cdb-pg/src/port -lpgport -lws2_32 -lgdi32
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
ifeq "$(BLD_ARCH)" "win32"
OPENSSL_LIBS=-L $(GPCC)/ext/$(OPENSSL) -lssl -lcrypto
else
OPENSSL_LIBS=-lssl -lcrypto
endif
GP_VERSION:=$(shell cat $(GPCC)/../VERSION)

# flag to include/exclude gpfdist transformation support
GPFXDIST=-DGPFXDIST

# use gpcc/ext/ copies of libraries until prebuilt ones are ready
ifeq "$(BLD_ARCH)" "win32"
BLD_THIRDPARTY_BIN_DIR=$(GPCC)/ext/bin
BLD_THIRDPARTY_OPENSSL_INCLUDES=-I$(GPCC)/ext/$(OPENSSL)/include   
endif
INCLUDES=-I$(BLD_THIRDPARTY_INCLUDE_DIR) -I$(GPCC)/ext/include $$($(BLD_THIRDPARTY_BIN_DIR)/apr-1-config --includes) -I$(GPCC)/src/include -I. $(BLD_THIRDPARTY_OPENSSL_INCLUDES) 
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

#all: mkext mkgpfdist mklibgpcc mkgpha mkfts
#all: mkdir mkext mkgpfdist mklibgpcc mkgp mklibchubby mkchubbytest mkpaxos mkgpha mkgpma
all: mkdir mkgpfdist

#mkgpma mkgp 

mkpaxos: mkdir
	(cd src/paxos;sh build)
	mkdir -p build/bin/lib/paxos
	cp src/paxos/bin/*.pyc build/bin/lib/paxos
	cp src/paxos/gppaxos build/bin
	chmod 755 build/bin/gppaxos

mkgpfdist: mkext
	@echo --- gpfdist --------------------------------------------------
	cd src/gpfdist/ && \
	  ln -sf ../../../cdb-pg/src/backend/utils/misc/fstream/*.c .
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

mklibchubby: src/build/lib/libchubby.a

src/build/lib/libchubby.a: mkext src/paxos/chubby/c/*.[ch]
	@echo --- libchubby --------------------------------------------------
	(cd src/paxos/chubby/c; $(CC) -c -I../include $(INCLUDES) $(CPPFLAGS) $(CFLAGS) -Wall -Werror *.c)
	@echo copying include files
	@(cp -f -r src/paxos/chubby/c/chubby.h src/build/include/ )
	@echo creating library
	@(cd src/paxos/chubby/c; rm -f ../../../build/lib/libchubby.a; \
		ar cru ../../../build/lib/libchubby.a *.o;	\
		ranlib ../../../build/lib/libchubby.a)

mkchubbytest: src/paxos/test/chubbytest

src/paxos/test/chubbytest: src/paxos/test/chubbytest.c src/build/lib/libchubby.a
	@echo --- chubbytest --------------------------------------------------
	(cd src/paxos/test; $(CC) -I../../build/include -L../../build/lib $(INCLUDES) $(CPPFLAGS) $(CFLAGS) -Wall -Werror -o chubbytest chubbytest.c -lchubby $(APULIBS) $(APRLIBS))

mkgp:
	@echo --- gp -------------------------------------------------------
	@mkdir -p build/bin
	@cp -f src/gp/gp build/bin/

mkgpma: mklibgpcc mklibchubby
	@echo --- gpma -----------------------------------------------------
	@(cd src/gpma; $(CC) $(INCLUDES) $(CPPFLAGS) $(CFLAGS) -I../build/include \
		-o gpma *.c -L../build/lib -lgpcc -levent -ljson -lresolv -lchubby $(OPENSSL_LIBS) $(APULIBS) $(APRLIBS))
	@mkdir -p build/bin
	@cp -f src/gpma/gpma build/bin/


mkgpha: mklibgpcc
	@echo --- gpha -----------------------------------------------------
	@(cd src/gpha; $(CC) $(INCLUDES) $(CPPFLAGS) $(CFLAGS) \
		-o gpha *.c -L../build/lib -lgpcc $(APULIBS) $(APRLIBS) -levent -ljson -lresolv $(OPENSSL_LIBS))
	@mkdir -p build/bin
	@cp -f src/gpha/gpha build/bin/

mkdir:
	@mkdir -p src/build/lib src/build/include build/bin build/bin/lib

mklibgpcc: mkext
	@echo --- libgpcc --------------------------------------------------
	@echo compiling
	@(cd src/libgpcc; $(CC) $(INCLUDES) $(CPPFLAGS) $(CFLAGS) -c *.c)
	@echo creating library
	@(cd src/libgpcc; rm -f ../build/lib/libgpcc.a; \
		ar cru ../build/lib/libgpcc.a *.o;	\
		ranlib ../build/lib/libgpcc.a)
	@echo copying include files
	@(cp -f -r src/include/* src/build/include/ )


mktest: mklibgpcc
	@echo --- test ------------------------------------------------------	
	(cd test; $(CC) $(INCLUDES) $(CPPFLAS) $(CFLAGS) \
			-o net1 net1.c -L../src/build/lib -lgpcc $(APULIBS) $(APRLIBS) -ljson -levent $(OPENSSL_LIBS) -lresolv)

mkext: mkdir
	$(MAKE) -C ext

veryclean: clean
	$(MAKE) -C ext veryclean

clean: 
	$(MAKE) -C ext veryclean
	rm -f src/gpfdist/gpfdist
	rm -rf src/build
	find src -name '*.o' -exec rm -f {} \;
