SHELL=/bin/bash

default: all

this_makefile := $(abspath $(lastword $(MAKEFILE_LIST)))
BLD_TOP := $(dir $(this_makefile))/../../

subdir=tools/gpnetbench
top_builddir=../../
include $(BLD_TOP)/src/Makefile.global

SERVER_OBJS=gpnetbenchServer.o
CLIENT_OBJS=gpnetbenchClient.o

all: gpnetbenchServer gpnetbenchClient
	@echo "finish building gpnetbenchServer and gpnetbenchClient"

gpnetbenchServer: $(SERVER_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o gpnetbenchServer $(SERVER_OBJS)

gpnetbenchClient: $(CLIENT_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o gpnetbenchClient $(CLIENT_OBJS)

install: all
	$(INSTALL_PROGRAM) gpnetbenchServer $(bindir)/lib
	$(INSTALL_PROGRAM) gpnetbenchClient $(bindir)/lib

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

clean:
	rm -rf *.o gpnetbenchServer gpnetbenchClient

distclean: clean

.PHONY: default all clean distclean
