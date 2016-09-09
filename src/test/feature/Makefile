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
override LDFLAGS += -L/usr/local/lib -L/usr/lib -L$(abs_top_srcdir)/src/test/feature/ -L$(abs_top_srcdir)/src/test/feature/lib/ -L$(abs_top_srcdir)/src/interfaces/libpq -L$(gtest_lib_path) -L$(gmock_lib_path)

PROG = test_main.cpp $(wildcard */*.cpp)
OBJS = $(patsubst %.cpp,%.o,$(PROG))
RM = rm -rf

.PHONY: all sharelib sharelibclean distclean clean doc

all: $(OBJS) sharelib
	$(MAKE) -C lib all
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LIBS) -o feature-test

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $? -o $@

sharelib:
	cd UDF/lib || exit 1; $(MAKE) || exit 2
	cd ExternalSource/lib || exit 1; $(MAKE) || exit 2

sharelibclean:
	cd UDF/lib || exit 1; $(RM) *.o *.so || exit 2
	cd ExternalSource/lib || exit 1; $(MAKE) || exit 2

doc:
	doxygen doxygen_template

clean distclean: sharelibclean
	find . -type f -name "*.out" -exec $(RM) {} +
	$(RM) ExternalSource/ans/external_oid.ans ExternalSource/ans/exttab1.ans ExternalSource/sql/external_oid.sql ExternalSource/sql/exttab1.sql
	$(RM) UDF/ans/function_c.ans UDF/ans/function_creation.ans UDF/sql/function_c.sql UDF/sql/function_creation.sql
	$(RM) testlib/ans/template.ans testlib/sql/template.sql
	$(RM) utility/ans/copytest.csv utility/ans/onek.data
	$(RM) feature-test
	$(RM) feature-test.dSYM
	$(RM) doc
	$(MAKE) -C lib clean
	$(RM) $(OBJS)
