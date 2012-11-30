SHELL=bash
BASEDIR=$(shell pwd)
CC=gcc
INCLUDES=-I$(BASEDIR)/include
CFLAGS=-Wall $$($(BASEDIR)/../bin/apr-1-config --cflags)
CPPFLAGS=$$($(BASEDIR)/../bin/apr-1-config --includes)
LDFLAGS=
OSNAME=$(shell uname -s)
SOURCES=sigar.c sigar_cache.c sigar_fileinfo.c sigar_format.c sigar_getline.c \
	sigar_ptql.c sigar_signal.c sigar_util.c
OBJS=sigar.o sigar_cache.o sigar_fileinfo.o sigar_format.o sigar_getline.o \
	sigar_ptql.o sigar_signal.o sigar_util.o

# OS Dependent setup
ifeq (Linux, $(OSNAME))
	INCLUDES+=-I$(BASEDIR)/src/os/linux
	SOURCES+=os/linux/linux_sigar.c
	OBJS+= linux_sigar.o
else
	ifeq (SunOS, $(OSNAME))
		INCLUDES+=-I$(BASEDIR)/src/os/solaris
		SOURCES+=os/solaris/solaris_sigar.c os/solaris/get_mib2.c \
			os/solaris/kstats.c os/solaris/procfs.c
		OBJS+=solaris_sigar.o get_mib2.o kstats.o procfs.o
		TEST_LDFLAGS=-lkstat -lnsl -lsocket
	else
		ifeq (Darwin, $(OSNAME))
			ifeq (9, $(shell uname -r | grep -o -e '^9'))
				CFLAGS+=-DDARWIN -DDARWIN_HAS_LIBPROC_H
			else
				CFLAGS+=-DDARWIN
			endif
			INCLUDES+=-I$(BASEDIR)/src/os/darwin -I/Developer/Headers/FlatCarbon
			SOURCES+=os/darwin/darwin_sigar.c
			OBJS+=darwin_sigar.o
			TEST_LDFLAGS=-framework CoreServices -framework IOKit
		endif
	endif
endif


all: libsigar

libsigar:
	@echo --- libsigar \($(OSNAME)\)----------------------------------
	(cd src; $(CC) $(INCLUDES) $(CPPFLAGS) $(CFLAGS) -c $(LIBS) $(LDFLAGS) $(SOURCES); \
	 ar -cvr libsigar.a $(OBJS);)

testapp: libsigar
	@echo --- libsigar test app --------------------------------------
	(cd test; $(CC) $(CPPFLAGS) $(CFLAGS) $(INCLUDES) -I$(BASEDIR)/include -L$(BASEDIR)/src $(TEST_LDFLAGS) -o sigtest sigtest.c -lsigar;)

veryclean: clean
	find . -name libsigar.a -exec rm -f {} \;
	find . -name sigtest -exec rm -f {} \;

clean:
	find . -name '*.o' -exec rm -f {} \;
