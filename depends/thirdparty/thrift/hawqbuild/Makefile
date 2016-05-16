subdir = depends/thirdparty/thrift
top_builddir = ../../../..
include Makefile.global

.PHONY: all install distclean maintainer-clean clean pre-config

ifeq ($(with_thrift), yes)

all: build
	cd $(top_srcdir)/$(subdir); mkdir -p install; \
	$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/install install

install:
	cd $(top_srcdir)/$(subdir) && $(MAKE) install

distclean:
	cd $(top_srcdir)/$(subdir) && $(MAKE) distclean
	cd $(top_srcdir)/$(subdir) && rm -rf install

maintainer-clean: distclean

clean:
	cd $(top_srcdir)/$(subdir) && $(MAKE) clean

build: pre-config
	cd $(top_srcdir)/$(subdir) && $(MAKE)

pre-config:
	cd $(top_srcdir)/$(subdir); \
	./bootstrap.sh; \
	./configure --without-php --prefix=$(prefix)

else

all install distclean maintainer-clean clean pre-config:

endif
