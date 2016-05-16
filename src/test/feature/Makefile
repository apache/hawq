top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

#.DEFAULT:
#$(MAKE) -C lib $@

include $(top_builddir)/src/Makefile.global

gtest_include = $(top_builddir)/depends/thirdparty/googletest/googletest/include
gmock_include = $(top_builddir)/depends/thirdparty/googletest/googlemock/include
gtest_lib_path = $(top_builddir)/depends/thirdparty/googletest/build/googlemock
gmock_lib_path = $(top_builddir)/depends/thirdparty/googletest/build/googlemock/gtest

override CXXFLAGS = -Wall -O0 -g -std=c++11
override CPPFLAGS := -I/usr/include -I/usr/local/include -I/usr/include/libxml2 -I$(top_builddir)/src/test/feature/ -I$(top_builddir)/src/test/feature/lib/ -I$(top_builddir)/src/interfaces/libpq -I$(top_builddir)/src/interfaces -I$(top_builddir)/src/include  -I$(gtest_include) -I$(gmock_include)

override LIBS := $(LIBS) -lgtest -lpq -lxml2 -ltest
override LDFLAGS += -L/usr/local/lib -L/usr/lib -L$(top_builddir)/src/test/feature/lib -L$(gtest_lib_path) -L$(gmock_lib_path)

PROG = test-main.cpp testlib/test-lib.cpp

.PHONY: all distclean  clean

all:
	$(MAKE) -C lib all
	g++ $(CPPFLAGS) $(CXXFLAGS) $(PROG) $(LDFLAGS) $(LIBS) -o feature-test

clean distclean:
	rm -f feature-test
	rm -rf feature-test.dSYM
	$(MAKE) -C lib clean
	
