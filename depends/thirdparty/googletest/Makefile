subdir = depends/thirdparty/googletest
top_builddir = ../../../
include Makefile.global

.PHONY: all install distclean maintainer-clean clean pre-config

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
	cd $(top_srcdir)/$(subdir)/ && mkdir -p build && cd build && cmake -DCMAKE_INSTALL_PREFIX=$(prefix) ..
