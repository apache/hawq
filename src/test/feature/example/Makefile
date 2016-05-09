subdir = src/test/feature/example
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CXXFLAGS = -Wall -O0 -g -std=c++11
override CPPFLAGS := -I/usr/include -I/usr/local/include -I$(top_builddir)/src/test/feature/
override LIBS := $(LIBS) -lgtest
override LDFLAGS += -L/usr/local/lib -L/usr/lib

PROG = test_example.cpp

all:
	g++ $(CPPFLAGS) $(CXXFLAGS) $(PROG) $(LDFLAGS) $(LIBS) -o test_example

distclean clean:
	rm -f test_example
