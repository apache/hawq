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
.PHONY: build all install distclean maintainer-clean clean pre-config

ifeq ($(with_libhdfs3), yes)

# Hack: For the target "all", we will install them temporarily under build/install to compile hawq
all: build
	cd $(top_srcdir)/$(subdir)/build; mkdir -p install; \
	$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/build/install install

install: build
	cd $(top_srcdir)/$(subdir)/build && $(MAKE) install

distclean:
	rm -rf $(top_srcdir)/$(subdir)/build

maintainer-clean: distclean

clean:
	if [ -d $(top_srcdir)/$(subdir)/build ]; then \
		cd $(top_srcdir)/$(subdir)/build && $(MAKE) clean && rm -f build.timestamp; \
	fi

build: pre-config
	cd $(top_srcdir)/$(subdir)/build && $(MAKE)

# trigger bootstrap only once.
pre-config:
	cd $(top_srcdir)/$(subdir)/; \
	mkdir -p build; \
	cd build; \
	if [ ! -f build.timestamp ]; then \
		$(abs_top_srcdir)/$(subdir)/bootstrap --prefix=$(prefix) $(PRE_CFG_ARG) && touch build.timestamp; \
	fi

else

all install distclean maintainer-clean clean pre-config:

endif
