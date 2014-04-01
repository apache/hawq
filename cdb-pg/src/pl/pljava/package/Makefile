all: gppkg

BLD_TOP=../../../../../
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/releng/tools.mk
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

PLJAVA_DIR=`cd .. && pwd`
PLJAVA_VER=1.2
PLJAVA_REL=1
PLJAVA_RPM_FLAGS="--define 'pljava_dir $(PLJAVA_DIR)' --define 'pljava_ver $(PLJAVA_VER)' --define 'pljava_rel $(PLJAVA_REL)'"
PLJAVA_RPM=pljava-$(PLJAVA_VER)-$(PLJAVA_REL).$(ARCH).rpm
PLJAVA_GPPKG=pljava-$(PLJAVA_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(PLJAVA_GPPKG)
EXTRA_CLEAN+=$(PLJAVA_RPM) $(PLJAVA_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg:
	$(MAKE) $(PLJAVA_RPM) RPM_FLAGS=$(PLJAVA_RPM_FLAGS)
	$(MAKE) $(PLJAVA_GPPKG) MAIN_RPM=$(PLJAVA_RPM)

.PHONY: gppkg
