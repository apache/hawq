top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

#.DEFAULT:
#$(MAKE) -C lib $@

include $(top_builddir)/src/Makefile.global

override CXXFLAGS = -Wall -O0 -g -std=c++11
override CPPFLAGS := -I/usr/include -I/usr/local/include -I/usr/include/libxml2 -I$(top_builddir)/src/test/feature/ -I$(top_builddir)/src/test/feature/lib/ -I$(top_builddir)/src/interfaces/libpq -I$(top_builddir)/src/interfaces -I$(top_builddir)/src/include

override LIBS := $(LIBS) -lgtest -lpq -lxml2 -ltest
override LDFLAGS += -L/usr/local/lib -L/usr/lib -L$(top_builddir)/src/test/feature/lib

PROG = test-main.cpp testlib/test-lib.cpp

.PHONY: all distclean  clean

all:
	$(MAKE) -C lib all
	g++ $(CPPFLAGS) $(CXXFLAGS) $(PROG) $(LDFLAGS) $(LIBS) -o feature-test

clean distclean:
	rm -f feature-test
	rm -rf feature-test.dSYM
	$(MAKE) -C lib clean
	
