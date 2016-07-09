subdir = src/test/feature/lib
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

gtest_include = $(top_builddir)/depends/thirdparty/googletest/googletest/include
gmock_include = $(top_builddir)/depends/thirdparty/googletest/googlemock/include
gtest_lib_path = $(top_builddir)/depends/thirdparty/googletest/build/googlemock
gmock_lib_path = $(top_builddir)/depends/thirdparty/googletest/build/googlemock/gtest

override CXX = g++
override CXXFLAGS = -Wall -O0 -g -std=c++11
override CPPFLAGS := -I/usr/include -I/usr/local/include -I/usr/include/libxml2 -I$(top_builddir)/src/interfaces/libpq -I$(top_builddir)/src/interfaces -I$(top_builddir)/src/include -I$(gtest_include) -I$(gmock_include)
override LIBS := $(LIBS) -lpq -lxml2
override LDFLAGS += -L/usr/local/lib -L/usr/lib -L$(gtest_lib_path) -L$(gmock_lib_path)

PROG = $(abspath $(wildcard *.cpp))
OBJS := $(patsubst %.cpp,%.o,$(PROG))
AR = ar -r
RM = rm -rf

all: $(OBJS)
	$(AR) libtest.a $?

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $? -o $@

distclean clean:
	$(RM) libtest.a
	$(RM) *.o

.PHONY: all distclean clean
