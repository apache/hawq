subdir = src/test/feature/lib
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CXXFLAGS = -Wall -O0 -g -c -std=c++11
override CPPFLAGS := -I/usr/include -I/usr/local/include -I/usr/include/libxml2 -I$(top_builddir)/src/interfaces/libpq -I$(top_builddir)/src/interfaces -I$(top_builddir)/src/include

override LIBS := $(LIBS) -lpq -lxml2
override LDFLAGS += -L/usr/local/lib -L/usr/lib

PROG = string-util.cpp psql.cpp command.cpp xml-parser.cpp hawq-config.cpp

all:
	g++ $(CPPFLAGS) $(CXXFLAGS) $(PROG) 
	ar -r libtest.a  *.o

distclean clean:
	rm -f libtest.a
	rm -f *.o

.PHONY: all distclean  clean
