all: gppkg

BLD_TOP=../../../../../
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/releng/tools.mk
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

R_VER=2.13.0
R_REL=1
R_DIR=$(RELENG_TOOLS)/R-$(R_VER)
R_RPM_FLAGS="--define 'r_dir $(R_DIR)' --define 'r_ver $(R_VER)' --define 'r_rel $(R_REL)'"
R_RPM=R-$(R_VER)-$(R_REL).$(ARCH).rpm
R_HOME=$(R_DIR)/lib64/R

PLR_DIR=`cd .. && pwd`
PLR_VER=1.0
PLR_REL=1
PLR_RPM_FLAGS="--define 'plr_dir $(PLR_DIR)' --define 'plr_ver $(PLR_VER)' --define 'plr_rel $(PLR_REL)' --define 'r_ver $(R_VER)' --define 'r_dir $(R_DIR)'"
PLR_RPM=plr-$(PLR_VER)-$(PLR_REL).$(ARCH).rpm
PLR_GPPKG=plr-$(PLR_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(PLR_GPPKG)
EXTRA_CLEAN+=$(R_RPM) $(PLR_RPM) $(PLR_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg: $(R_DIR)
	$(MAKE) $(R_RPM) RPM_FLAGS=$(R_RPM_FLAGS)
	$(MAKE) $(PLR_RPM) RPM_FLAGS=$(PLR_RPM_FLAGS)
	$(MAKE) $(PLR_GPPKG) MAIN_RPM=$(PLR_RPM) DEPENDENT_RPMS=$(R_RPM)

.PHONY: gppkg
