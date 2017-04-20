
# this file copied and adapted from PostgreSQL source
# to allow easy build on BSD systems

all install uninstall clean distclean maintainer-clean test check installcheck docs docs-install docs-uninstall utils: GNUmakefile
	@IFS=':' ; \
	 for dir in $$PATH; do \
	   for prog in gmake gnumake make; do \
	     if [ -f $$dir/$$prog ] && ( $$dir/$$prog -f /dev/null --version 2>/dev/null | grep GNU >/dev/null 2>&1 ) ; then \
	       GMAKE=$$dir/$$prog; \
	       break 2; \
	     fi; \
	   done; \
	 done; \
	\
	 if [ x"$${GMAKE+set}" = xset ]; then \
	   echo "Using GNU make found at $${GMAKE}"; \
	   $${GMAKE} $@ ; \
	 else \
	   echo "You must use GNU make to build PostGIS." ; \
	   false; \
	 fi

configure: configure.in
	./autogen.sh

BLD_TOP=../../..
include $(BLD_TOP)/Makefile.global     # for BLD_ARCH
include $(BLD_TOP)/releng/tools.mk
GEOS_DIR=$(RELENG_TOOLS)/geos-3.3.8
GEOS_CONFIG=$(GEOS_DIR)/bin/geos-config
PROJ_DIR=$(RELENG_TOOLS)/proj-4.8.0
PG_CONFIG=`pwd`/$(BLD_TOP)/../src/bin/pg_config/pg_config

# libxml2 
EXT_TOP=`pwd`/$(BLD_TOP)/ext/$(BLD_ARCH)
XML2_CONFIG=$(EXT_TOP)/bin/xml2-config

OLD_GEOS_PREFIX=$(shell $(GEOS_CONFIG) --prefix)

GNUmakefile: GNUmakefile.in $(GEOS_DIR) $(PROJ_DIR)
	perl -i.bak -p -e "s|$(OLD_GEOS_PREFIX)|$(GEOS_DIR)|g" ${GEOS_DIR}/lib/*.la 2>/dev/null
	perl -i.bak -p -e 's|^prefix=(.*)|prefix=$(EXT_TOP)|' $(XML2_CONFIG) 2>/dev/null
	GEOS_PREFIX=$(GEOS_DIR) ./configure --with-geosconfig=$(GEOS_CONFIG) --with-projdir=$(PROJ_DIR) --with-pgconfig=$(PG_CONFIG) --without-raster --with-xml2config=$(XML2_CONFIG)
