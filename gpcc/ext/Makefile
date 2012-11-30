#         $Id: $
#     $Change: $
#   $DateTime: $
#     $Author: $

# protect this makefile's default target from targets in Makefile.global and Makefile.thirdparty
default: all

BLD_TOP=../..
include $(BLD_TOP)/Makefile.global
include $(BLD_TOP)/Makefile.thirdparty

# NO_M64 set in Makefile.global turns off default to 64-bit
# here, select bitness we desire this build for gcc and ld
export CC=$(strip gcc $(BLD_CFLAGS))
export LDFLAGS=$(BLD_CFLAGS)

SHELL=bash
GPCC=$(shell echo `pwd`/..)
CDBINCL=-I$(GPCC)/../cdb-pg/src/include
OPT=-g -O3

ifeq "$(BLD_ARCH)" "win32"
APR=apr-1.3.3
APU=apr-util-1.3.4
else
APR=apr-1.2.12
APU=apr-util-1.2.12
endif

JSON=json-c-0.6
LIBEVENT=libevent-1.4.6-stable
OPENSSL=openssl-0.9.8r

ifeq "$(BLD_ARCH)" "win32"
MINGWRANLIB=/usr/local/mingw32/mingw32/bin/ranlib
CONFFLAGS=--build=x86_64-unknown-linux-gnu --host=i686-pc-mingw32 --without-zlib
CC=/usr/local/mingw32/bin/mingw32-gcc
LDFLAGS=-L/usr/local/mingw32/mingw32/lib
CPPFLAGS=-I/usr/local/mingw32/mingw32/include
endif

ifeq "$(BLD_ARCH)" "win32"
all: mkdir mklibssl mkdbgen mklibapr mklibapu mklibevent mklibjson
else
all: mkdir mkdbgen
endif

# ------------------------------------------------------------------------------------------------
#
# Optionally, define a Configure target.  OpenSSL's config script doesn't let us control which
# platform we actually want to build for.  We have to send the platform and any options to the
# Configure script, ourselves.
#
# ------------------------------------------------------------------------------------------------

rhel5_x86_32_BLD_TARGET=linux-elf -m32
sol10_x86_64_BLD_TARGET=solaris64-x86_64-gcc
sol10_x86_32_BLD_TARGET=solaris-x86-gcc -m32
sol10_sparc_BLD_TARGET=solaris64-sparcv9-gcc -m64
sol10_sparc_64_BLD_TARGET=solaris64-sparcv9-gcc -m64
BLD_TARGET=$($(BLD_ARCH)_BLD_TARGET)

ifeq "$(BLD_TARGET)" ""
CONFIG_OPENSSL=./config
else
CONFIG_OPENSSL=./Configure $(BLD_TARGET)
endif

ifeq "$(BLD_ARCH)" "win32"
CONFIG_OPENSSL=./Configure mingw
endif

mkdir:
	@echo --- ext ------------------------------------------------------
	@echo $(GPCC)
	mkdir -p lib include

mklibjson: mkdir lib/libjson.a
mklibevent: mkdir lib/libevent.a
mklibssl: mkdir lib/libssl.a
mklibapr: mkdir lib/libapr-1.a
mklibapu: mkdir lib/libaprutil-1.a

lib/libjson.a:
	@echo     -- json
	if ! [ -a $(JSON)/Makefile ]; then \
		gzip -d -c $(JSON).tar.gz | tar xf - ;\
		(cd $(JSON) && CC="$(CC)" LDFLAGS="$(LDFLAGS)" CPPFLAGS="$(CPPFLAGS)" \
		  ./configure --enable-shared=no $(CONFFLAGS) \
		  --enable-static=yes --enable-threads --prefix=$(GPCC)/ext) >LOG.$(@F)-configure 2>&1 ;\
	    retval=$$?; \
	    if [ $${retval} != 0 ]; then \
	        rm -f $(JSON)/Makefile; \
	        cat LOG.$(@F)-configure; \
	        exit $${retval}; \
	    fi ; \
	fi
	(cd $(JSON) && $(MAKE) all && $(MAKE) install) >LOG.$(@F)-make 2>&1 ; \
	retval=$$?; \
	if [ $${retval} != 0 ]; then \
	    cat LOG.$(@F)-make; \
	    exit $${retval}; \
	fi
	if [ "$(BLD_ARCH)" == "win32" ]; then \
		$(MINGWRANLIB) $(GPCC)/ext/lib/libjson.a; \
	fi


lib/libevent.a:
	@echo     -- libevent
	if ! [ -a $(LIBEVENT)/Makefile ]; then \
		gzip -d -c $(LIBEVENT).tar.gz | tar xf - ;\
		(cd $(LIBEVENT) && CC="$(CC)" LDFLAGS="$(LDFLAGS)" CPPFLAGS="$(CPPFLAGS)" \
	          ./configure --enable-shared=no $(CONFFLAGS) \
	          --enable-static=yes --enable-threads --prefix=$(GPCC)/ext) >LOG.$(@F)-configure 2>&1 ;\
	    retval=$$?; \
	    if [ $${retval} != 0 ]; then \
	        rm -f $(LIBEVENT)/Makefile; \
	        cat LOG.$(@F)-configure; \
	        exit $${retval}; \
	    fi ; \
	    if [ x"$(BLD_ARCH)" == x"win32" ]; then \
		    patch -p0 < libevent1.patch; \
		    patch $(LIBEVENT)/Makefile < libevent2.patch; \
	    fi; \
	fi
	cd $(LIBEVENT) && $(MAKE) install >LOG.$(@F)-make 2>&1 ; \
	retval=$$?; \
	if [ $${retval} != 0 ]; then \
	    cat LOG.$(@F)-make; \
	    exit $${retval}; \
	fi
	if [ "$(BLD_ARCH)"x == "win32"x ]; then \
		$(MINGWRANLIB) $(GPCC)/ext/lib/libevent.a; \
	fi

lib/libssl.a:
	# openssl is compiled only on win32 (see all:)
	@echo     -- openssl
	if [ ! -f $(OPENSSL)/Makefile ]; then \
		gzip -d -c $(OPENSSL).tar.gz | tar xf - ;\
		(cd $(OPENSSL) && CC="$(CC)" LDFLAGS="$(LDFLAGS)" CPPFLAGS="$(CPPFLAGS)" \
	          $(CONFIG_OPENSSL) no-shared threads --prefix=$(GPCC)/ext ) \
		  >LOG.$(@F)-configure 2>&1 ;\
	    retval=$$?; \
	    if [ $${retval} != 0 ]; then \
	        rm -f $(OPENSSL)/Makefile; \
	        cat LOG.$(@F)-configure; \
	        exit $${retval}; \
	    fi ; \
	fi

	cd $(OPENSSL) && $(MAKE) clean build_libs >LOG.$(@F)-make 2>&1 ; \
	retval=$$?; \
	if [ $${retval} != 0 ]; then \
	    cat LOG.$(@F)-make; \
	    exit $${retval}; \
	fi;

	# copy the following files to ext/lib directroy - this is a workaround, since '--prefix' was ignored during './configure'
	cp -f $(OPENSSL)/libssl.a $(GPCC)/ext/lib
	cp -f $(OPENSSL)/libcrypto.a  $(GPCC)/ext/lib
	$(MINGWRANLIB) $(GPCC)/ext/lib/libssl.a;
	$(MINGWRANLIB) $(GPCC)/ext/lib/libcrypto.a;

lib/libaprutil-1.a: lib/libapr-1.a
	@echo     -- apu
	if ! [ -a $(APU)/Makefile ]; then \
		gzip -d -c $(APU).tar.gz | tar xf - ;\
		(cd $(APU) && CC="$(CC)" LDFLAGS="$(LDFLAGS)" CPPFLAGS="$(CPPFLAGS)" \
		  ./configure --enable-shared=no $(CONFFLAGS) --enable-static=yes \
		  --enable-threads --with-apr=../$(APR) --prefix=$(GPCC)/ext \
	      --with-expat=builtin)	>LOG.$(@F)-configure 2>&1 ;\
	    retval=$$?; \
	    if [ $${retval} != 0 ]; then \
	        rm -f $(APU)/Makefile; \
	        cat LOG.$(@F)-configure; \
	        exit $${retval}; \
	    fi ; \
	fi
	cd $(APU) && $(MAKE) all install >LOG.$(@F)-make 2>&1 ; \
	retval=$$?; \
	if [ $${retval} != 0 ]; then \
	    cat LOG.$(@F)-make; \
	    exit $${retval}; \
	fi

	if [ "$(BLD_ARCH)"x == "win32"x ]; then \
		$(MINGWRANLIB) $(GPCC)/ext/lib/libaprutil-1.a; \
		$(MINGWRANLIB) $(GPCC)/ext/lib/libexpat.a; \
	fi


lib/libapr-1.a:
	@echo     -- apr
	if ! [ -a $(APR)/Makefile ]; then \
	    gzip -d -c $(APR).tar.gz | tar xf - ;\
	    if [ x"$(BLD_ARCH)" = x"win32" ] ; then \
	        patch $(APR)/configure.in < apr1.patch; \
	        (cd $(APR) && autoconf) >LOG.$(@F)-autoconf 2>&1 ; \
	        retval=$$?; \
	        if [ $${retval} != 0 ]; then \
	            rm -f $(APR)/Makefile; \
	            cat LOG.$(@F)-autoconf; \
	            exit $${retval}; \
	        fi ; \
		patch $(APR)/include/arch/win32/apr_arch_misc.h < apr2.patch; \
		patch $(APR)/file_io/win32/open.c < apr3.patch; \
	    fi ; \
	    (cd $(APR) && CC="$(CC)" LDFLAGS="$(LDFLAGS)" CPPFLAGS="$(CPPFLAGS)" \
	      ./configure --enable-shared=no $(CONFFLAGS) --enable-static=yes \
	      --enable-threads --prefix=$(GPCC)/ext) >LOG.$(@F)-configure 2>&1 ;\
	    retval=$$?; \
	    if [ $${retval} != 0 ]; then \
	        rm -f $(APR)/Makefile; \
	        cat LOG.$(@F)-configure; \
	        exit $${retval}; \
	    fi ; \
	fi
	cd $(APR) && $(MAKE) all install >LOG.$(@F)-make 2>&1 ; \
	retval=$$?; \
	if [ $${retval} != 0 ]; then \
	    cat LOG.$(@F)-make; \
	    exit $${retval}; \
	fi

	if [ "$(BLD_ARCH)"x == "win32"x ]; then \
		$(MINGWRANLIB) $(GPCC)/ext/lib/libapr-1.a; \
	fi



mkdbgen:
ifneq "$(BLD_ARCH)" "win32"
	@echo     -- dbgen
	cd tpch && $(MAKE) CC="$(CC)" LDFLAGS="$(LDFLAGS)"
	cd tpch && cp -f dbgen ../../build/bin/
	cd tpch && cp -f dists.dss ../../build/bin/
endif

veryclean:
	rm -rf lib bin include build1 share ssl build-1
	rm -rf $(APR) $(APU) $(JSON) $(LIBEVENT) $(OPENSSL)

clean:
	rm -rf lib bin include build1 share ssl build-1
	(cd $(APR) && $(MAKE) clean; rm -f make.out)
	(cd $(APU) && $(MAKE) clean; rm -f make.out)
	(cd $(JSON) && $(MAKE) clean; rm -f make.out)
	(cd $(LIBEVENT) && $(MAKE) clean; rm -f make.out)
	(cd $(OPENSSL) && $(MAKE) clean; rm -f make.out)

