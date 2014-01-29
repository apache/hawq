#-------------------------------------------------------------------------
# Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
# Distributed under the terms shown in the file COPYRIGHT
# found in the root folder of this project or at
# http://eng.tada.se/osprojects/COPYRIGHT.html
#
# @author Thomas Hallgren
#-------------------------------------------------------------------------
subdir       = src/pl/pljava/src/C/pljava
top_builddir = $(MODULEROOT)/../../../../..
include $(top_builddir)/src/Makefile.global

NAME	:= pljava
SRCDIR	:= $(MODULEROOT)/pljava
INCLDIR	:= -I$(MODULEROOT)/include -I$(JNIDIR)

mkobjs	= $(subst $(SRCDIR)/,,$(1:%.c=%.o))
SRCS	= $(shell find $(SRCDIR) -name CVS -prune -o -type f -name \*.c -print)
OBJS	= $(call mkobjs,$(SRCS))

DLLTOOL_DEFFLAGS := --add-stdcall-alias

.PHONY: checkjavahome build pljava-plugin

$(OBJS): %.o : $(SRCDIR)/%.c
	@-mkdir -p $(@D)
	$(CC) -c $(CFLAGS) $(CPPFLAGS) $< -o $@

SS_VERSION := $(subst ., ,$(subst devel,.99,$(subst beta,.99,$(subst RC,.99,$(VERSION)))))
PGSQL_MAJOR_VER	= $(word 1,$(SS_VERSION))
PGSQL_MINOR_VER	= $(word 2,$(SS_VERSION))
PGSQL_PATCH_VER	= $(word 3,$(SS_VERSION))

PLJAVA_CPPFLAGS = \
	-DPKGLIBDIR=\"$(pkglibdir)\" $(INCLDIR) \
	-DPGSQL_MAJOR_VER=$(PGSQL_MAJOR_VER) \
	-DPGSQL_MINOR_VER=$(PGSQL_MINOR_VER) \
	-DPGSQL_PATCH_VER=$(PGSQL_PATCH_VER)

ifeq ($(PORTNAME), win32)
	DLL_BUILD := 1
else
ifeq ($(PORTNAME), darwin)
    #
    # override with JAVAVM_FWX_ROOT=/... on command line
    # if something else is needed
    #
	PLJAVA_CPPFLAGS += -I$(JAVA_HOME)/include
	PLJAVA_CPPFLAGS += -I$(JAVA_HOME)/include/darwin
else
ifeq ($(PORTNAME), ibmos2)
	DLL_BUILD := 1
else
	JRE_INCL := $(PORTNAME)
	ifeq ($(host_cpu), i686)
		JRE_CPU := i386
	else
	ifeq ($(host_cpu), i586)
		JRE_CPU := i386
	else
	ifeq ($(host_cpu), i486)
		JRE_CPU := i386
	else
	ifeq ($(host_cpu), sparc)
		JRE_CPU := sparcv9
	else
	ifeq ($(host_cpu), x86_64)
		JRE_CPU := amd64
	else
		JRE_CPU := $(host_cpu)
	endif
	endif
	endif
	endif
	endif

endif
endif
endif

ifdef USE_GCJ
	PLJAVA_CPPFLAGS += -DGCJ
	SHLIB_LINK = -lgcj
	ifdef DLL_BUILD
		SHLIB_LINK += -lws2_32
	endif
	OBJS += pljava_jar.o
else
	pljava-jar = pljava.jar
	ifeq ($(PORTNAME), darwin)
		SHLIB_LINK += -L. -framework JavaVM
	else
		ifdef JAVA_HOME
			ifeq ($(PORTNAME), win32)
				JDK_HOME := $(subst \,/,$(JAVA_HOME))
			else
				JDK_HOME := $(JAVA_HOME)
			endif
		else
			ifeq ($(PORTNAME), ibmos2)
				JAVA_HOME_TEST_RESULT := $(echo "You must set JAVA_HOME")
			else
				JAVA_HOME_TEST_RESULT := $(error You must set JAVA_HOME)
			endif
		endif
		ifdef DLL_BUILD
			ifeq ($(PORTNAME),ibmos2)
				JRE_INCL := os2
			else
				JRE_INCL := win32
			endif
			JVM_LIB  := $(JDK_HOME)/jre/bin/client
		else
			JRE_LIB := $(JDK_HOME)/jre/lib/$(JRE_CPU)
			JVM_LIB := $(firstword $(shell /bin/ls -d \
				$(JRE_LIB)/client \
				$(JRE_LIB)/server \
				$(JRE_LIB)/jrockit \
				2> /dev/null))
		endif

		PLJAVA_CPPFLAGS += -I"$(JDK_HOME)/include" -I"$(JDK_HOME)/include/$(JRE_INCL)"
		SHLIB_LINK += -L. -L"$(JVM_LIB)" -ljvm
	endif
endif


override CPPFLAGS += $(PLJAVA_CPPFLAGS)
override CFLAGS += $(PLJAVA_CFLAGS)

# import info for compiling shared libraries
include $(top_srcdir)/src/Makefile.shlib

all: build_all

install: build_install

uninstall: build_uninstall

build_all: all-lib

build_install: build_all install-lib

build_uninstall: uninstall-lib
#	rm -f $(DESTDIR)$(pkglibdir)/$(plugin)
