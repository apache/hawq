##-------------------------------------------------------------------------------------
##
## Copyright (C) 2011 EMC - Data Computing Division (DCD)
##
## @doc: Engineering Services makefile utilities
##
## @author: eespino
##
##-------------------------------------------------------------------------------------

.PHONY: opt_write_test sync_tools clean_tools

##-------------------------------------------------------------------------------------
## dependent modules
##
## NOTE: Dependent project module version is kept in $(BLD_TOP)/releng/make/dependencies/ivy.xml
##-------------------------------------------------------------------------------------

GREP_SED_VAR = $(BLD_TOP)/releng/make/dependencies/ivy.xml | sed -e 's|\(.*\)rev="\(.*\)" conf\(.*\)|\2|'

## ---------------------------------------
## R-Project support
## ---------------------------------------

R_VER = $(shell grep 'name="R"' $(GREP_SED_VAR))

ifneq "$(wildcard /opt/releng/tools/R-Project/R/$(R_VER)/$(BLD_ARCH)/lib64)" ""
R_HOME = /opt/releng/tools/R-Project/R/$(R_VER)/$(BLD_ARCH)/lib64/R
else
ifneq "$(wildcard /opt/releng/tools/R-Project/R/$(R_VER)/$(BLD_ARCH)/lib)" ""
R_HOME = /opt/releng/tools/R-Project/R/$(R_VER)/$(BLD_ARCH)/lib/R
endif
endif

display_dependent_vers:
	@echo ""
	@echo "======================================================================"
	@echo " R_HOME ........ : $(R_HOME)"
	@echo " R_VER ......... : $(R_VER)"
	@echo " CONFIGFLAGS ... : $(CONFIGFLAGS)"
	@echo "======================================================================"

## ----------------------------------------------------------------------
## Sync/Clean tools
## ----------------------------------------------------------------------
## Populate/clean up dependent releng supported tools.  The projects are
## downloaded and installed into /opt/releng/...
##
## Tool dependencies and platform config mappings are defined in:
##   * Apache Ivy dependency definition file
##       releng/make/dependencies/ivy.xml
## ----------------------------------------------------------------------

opt_write_test:
	@if [ ! -w /opt ]; then \
	    echo ""; \
	    echo "======================================================================"; \
	    echo "ERROR: /opt is not writable."; \
	    echo "----------------------------------------------------------------------"; \
	    echo "  Supporting tools are stored in /opt.  Please ensure you have"; \
	    echo "  write access to /opt"; \
	    echo "======================================================================"; \
	    echo ""; \
	    exit 1; \
	fi

/opt/releng/apache-ant: 
	${MAKE} opt_write_test
	echo "Sync Ivy project dependency management framework ..."
	curl --silent http://releng.sanmateo.greenplum.com/tools/apache-ant.1.8.1.tar.gz -o /tmp/apache-ant.1.8.1.tar.gz
	( umask 002; [ ! -d /opt/releng ] && mkdir -p /opt/releng; \
	   cd /opt/releng; \
	   gunzip -qc /tmp/apache-ant.1.8.1.tar.gz | tar xf -; \
	   rm -f /tmp/apache-ant.1.8.1.tar.gz; \
	   chmod -R a+w /opt/releng/apache-ant )

sync_tools: opt_write_test /opt/releng/apache-ant
	@cd releng/make/dependencies; \
	 (umask 002; /opt/releng/apache-ant/bin/ant -DBLD_ARCH=$(BLD_ARCH) resolve); \
	 echo "Resolve finished"

clean_tools: opt_write_test
	@cd releng/make/dependencies; \
	/opt/releng/apache-ant/bin/ant clean; \
	rm -rf /opt/releng/apache-ant; \
