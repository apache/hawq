top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

gtest_include = $(abs_top_srcdir)/depends/thirdparty/googletest/googletest/include
gmock_include = $(abs_top_srcdir)/depends/thirdparty/googletest/googlemock/include
gtest_lib_path = $(abs_top_srcdir)/depends/thirdparty/googletest/build/googlemock
gmock_lib_path = $(abs_top_srcdir)/depends/thirdparty/googletest/build/googlemock/gtest

override CXX = g++
override CXXFLAGS = -Wall -O0 -g -std=c++11
override CPPFLAGS := -I/usr/include -I/usr/local/include -I/usr/include/libxml2 -I$(abs_top_srcdir)/src/test/feature/ -I$(abs_top_srcdir)/src/test/feature/lib/ -I$(abs_top_srcdir)/src/interfaces/libpq -I$(abs_top_srcdir)/src/interfaces -I$(abs_top_srcdir)/src/include  -I$(gtest_include) -I$(gmock_include)
override LIBS := $(LIBS) -lgtest -lpq -lxml2 -ltest
override LDFLAGS += -L/usr/local/lib -L/usr/lib -L$(abs_top_srcdir)/src/test/feature/lib -L$(gtest_lib_path) -L$(gmock_lib_path)

PROG = test-main.cpp $(wildcard */*.cpp)
RM = rm -rf

.PHONY: all distclean clean doc

all:
	$(MAKE) -C lib all
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(PROG) $(LDFLAGS) $(LIBS) -o feature-test

doc:
	doxygen doxygen_template

clean distclean:
	$(RM) feature-test
	$(RM) feature-test.dSYM
	$(RM) doc
	$(MAKE) -C lib clean
	
