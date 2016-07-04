top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OS = $(shell uname)

CXX = gcc 
CXXFLAGS = -Wall -O1 -g -std=gnu99 -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wformat-security -fno-strict-aliasing -fwrapv -fPIC

CC = gcc
CFLAGS = -Wall -O1 -g -std=gnu99 -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wformat-security -fno-strict-aliasing -fwrapv -fPIC

CPPFLAGS = -I$(abs_top_srcdir)/src/include
CPPFLAGS += -I$(abs_top_srcdir)/depends/libhdfs3/build/install$(prefix)/include
CPPFLAGS += -I$(abs_top_srcdir)/depends/libyarn/build/install$(prefix)/include

LDFLAGS = -L$(libdir)
LDFLAGS += -L$(abs_top_srcdir)/src/port
LDFLAGS += -L$(abs_top_builddir)/src/port
LDFLAGS += -L$(abs_top_srcdir)/depends/libhdfs3/build/install$(prefix)/lib
LDFLAGS += -L$(abs_top_srcdir)/depends/libyarn/build/install$(prefix)/lib

POSTGRES = $(abs_top_srcdir)/src/backend/postgres

PROG = function.c
OBJS = function.o
TARGET = function.so

RM = rm -rf 

all: $(OBJS) $(TARGET)

$(OBJS): $(PROG)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $(OBJS) $(PROG)

$(TARGET):
ifeq ($(OS),Darwin)
	$(CXX) $(CXXFLAGS) -bundle $(OBJS) -bundle_loader $(POSTGRES) $(LDFLAGS) -o $@
else
	$(CXX) $(CXXFLAGS) -shared $(OBJS) $(LDFLAGS) -Wl,-rpath,'$(abs_top_builddir)/lib' -o $@
endif

clean:
	$(RM) *.o
