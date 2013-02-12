# Makefile for Demo
#
#
SHELL=/bin/bash

#
# VARIABLES
#
MASTER_PORT=18501
PORT_BASE=18506

all: clean check cluster probe

cluster:
	@ echo ""
	@echo "Building cluster, this may take a few minutes."
	@MASTER_DEMO_PORT=$(MASTER_PORT) DEMO_PORT_BASE=$(PORT_BASE) \
		./demo_cluster.sh; if [ $$? -gt 1 ] ; then $(MAKE) clusterfail ; fi
	@echo ""

clusterfail:
	@echo "Failed to initialize GPDB, please check log files"
	@exit 1

probe:
	@ echo "Probing the databases "
	@ echo ""
	@ MASTER_DEMO_PORT=$(MASTER_PORT) DEMO_PORT_BASE=$(PORT_BASE) \
		./probe_config.sh
	@ echo "Probe finished"

.PHONY : clean 

check: 
	@ echo ""
	@ echo "Checking port availability... "
	@ echo ""
	@MASTER_DEMO_PORT=$(MASTER_PORT) DEMO_PORT_BASE=$(PORT_BASE)  \
		./demo_cluster.sh -c
	@echo ""
	@ echo "Checking completed without any errors...."
	@echo ""
		
clean:
	@ echo "Deleting cluster.... "
	@DEMO_PORT_BASE=$(PORT_BASE) ./demo_cluster.sh -d
	@echo ""


