ROOT = ..

COMPONENTS = \
			 pxf-service \
			 pxf-hbase \
			 pxf-hdfs \
			 pxf-hive \
			 stack \
			 tcserver \

PXF_ARTIFACTS = ../../../build/distributions
FETCH = $(ROOT)/tools/fetchBuild.sh

PHD_VERSION = undefined
PHD_URL = http://dist.dh.greenplum.com/dist/PHD/testing

TCSERVER_VERSION=2.9.7
TCSERVER_URL_BASE=http://build-prod.dh.greenplum.com/releases/tcServer
TCSERVER_URL=$(TCSERVER_URL_BASE)/$(TCSERVER_VERSION)

TARGET = $(COMPONENTS:%=%.fetched)

.PHONY: all
all: $(TARGET)

.PHONY: pxf-only
pxf-only: $(patsubst %, %.fetched, $(filter pxf-%, $(COMPONENTS)))

.PHONY: clean
clean:
	@echo clean does not do much, if want to remove tar files, use clean-tars

.PHONY: clean-tars
clean-tars:
	-rm -f *.log *.tar.gz *~ $(TARGET)

stack.fetched:
	echo Downloading PHD stack of version $(PHD_VERSION)
ifeq ($(PHD_VERSION),undefined)
	$(error PHD_VERSION undefined)
endif
	$(FETCH) $(PHD_URL) "PHD-$(PHD_VERSION)[\.0-9]*-bin-[0-9]*"
	touch $@

tcserver.fetched:
	echo Downloading tcServer of version $(TCSERVER_VERSION)
	$(FETCH) $(TCSERVER_URL) "vfabric-tc-server-standard-$(TCSERVER_VERSION).*"
	touch $@

%.fetched: $(PXF_ARTIFACTS)/%*.tar.gz %.single
	cp $< .
	touch $@

%.single:
	find $(PXF_ARTIFACTS) -iname "$**.tar.gz" | wc -l | grep -q 1
