all: gppkg

BLD_TOP=../../../../../
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/releng/tools.mk
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

JRE_VER=1.6.0_32
JRE_REL=1
JRE_DIR=$(RELENG_TOOLS)/jre-$(JRE_VER)
JRE_RPM_FLAGS="--define 'jre_dir $(JRE_DIR)' --define 'jre_ver $(JRE_VER)' --define 'jre_rel $(JRE_REL)'" 
JRE_RPM=jre-$(JRE_VER)-$(JRE_REL).$(ARCH).rpm

GPHDFS_DIR=`cd .. && pwd`
GPHDFS_VER=1.3
GPHDFS_REL=1
GPHDFS_RPM_FLAGS="--define 'gphdfs_dir $(GPHDFS_DIR)' --define 'gphdfs_ver $(GPHDFS_VER)' --define 'gphdfs_rel $(GPHDFS_REL)' --define 'jre_ver $(JRE_VER)'"
GPHDFS_RPM=gphdfs-$(GPHDFS_VER)-$(GPHDFS_REL).$(ARCH).rpm
GPHDFS_GPPKG=gphdfs-$(GPHDFS_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(GPHDFS_GPPKG)
EXTRA_CLEAN+=$(JRE_RPM) $(GPHDFS_RPM) $(GPHDFS_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg: $(JRE_DIR)
	$(MAKE) $(JRE_RPM) RPM_FLAGS=$(JRE_RPM_FLAGS)
	$(MAKE) $(GPHDFS_RPM) RPM_FLAGS=$(GPHDFS_RPM_FLAGS)
	$(MAKE) $(GPHDFS_GPPKG) MAIN_RPM=$(GPHDFS_RPM) DEPENDENT_RPMS=$(JRE_RPM)

.PHONY: gppkg
