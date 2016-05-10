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
.PHONY: all install distclean maintainer-clean clean pre-config

ifeq ($(with_libhdfs3), yes)

all: pre-config
	cd $(top_srcdir)/$(subdir)/build && $(MAKE)

install: all 
	cd $(top_srcdir)/$(subdir)/build && $(MAKE) install

distclean:
	rm -rf $(top_srcdir)/$(subdir)/build

maintainer-clean: distclean

clean:
	if [ -d $(top_srcdir)/$(subdir)/build ]; then \
		cd $(top_srcdir)/$(subdir)/build && $(MAKE) clean; \
	fi

pre-config:
	cd $(top_srcdir)/$(subdir)/ && mkdir -p build && cd build && ../bootstrap --prefix=$(prefix) $(PRE_CFG_ARG)

else

all install distclean maintainer-clean clean pre-config:

endif
