# -*-makefile-*-
#------------------------------------------------------------------------------
# A makefile that integrate building this module with hawq
#------------------------------------------------------------------------------
subdir = depends/libhdfs3
top_builddir = ../../
include Makefile.global

PRE_CFG_ARG = 
# get argument for running ../boostrap
ifeq ($(enable_debug), yes)
	PRE_CFG_ARG += --enable-debug
endif # enable_debug

ifeq ($(enable_coverage), yes)
	PRE_CFG_ARG += --enable-coverage
endif # enable_coverage

##########################################################################
#
.PHONY: all install make pre-config

all : make

install: make
	cd $(top_srcdir)/$(subdir)/build && $(MAKE) -j8 install

make: pre-config
	cd $(top_srcdir)/$(subdir)/build && $(MAKE) -j8

pre-config:
	cd $(top_srcdir)/$(subdir)/ && mkdir -p build && cd build && ../bootstrap --prefix=$(prefix) $(PRE_CFG_ARG)

