ROOT = ../..

COMPONENTS = \
			 pxf-service \
			 pxf-hbase \
			 pxf-hdfs \
			 pxf-hive \
			 stack \
			 tcserver \

ARTIFACTS = ../../../../build/distributions
DISTSVR_FETCH = $(ROOT)/tools/fetchBuild.sh

TCSERVER_VERSION=2.9.5/vfabric-tc-server-standard-2.9.5.SR1.tar.gz
TCSERVER_URL_BASE=http://build-prod.dh.greenplum.com/releases/tcServer

DISTSVR_VIEW = testing
DISTSVR_stack = $(DISTSVR_VIEW)

TARGET = $(COMPONENTS:%=%.fetched)

PHD_VERSION = undefined

.PHONY: all
all: $(TARGET)

.PHONY: pxf-only
pxf-only: $(patsubst %, %.fetched, $(filter pxf-%, $(COMPONENTS)))

.PHONY: clean
clean:
	-rm -rf $(TARGET)
	-rm -f *~

stack.fetched:
	echo Downloading PHD of version $(PHD_VERSION)
ifeq ($(PHD_VERSION),undefined)
	$(error PHD_VERSION undefined)
endif
	$(DISTSVR_FETCH) $(DISTSVR_stack) "PHD-$(PHD_VERSION)[\.0-9]*-bin-[0-9]*"
	touch $@

tcserver.fetched:
	wget -a tcserver.fetch.log $(TCSERVER_URL_BASE)/$(TCSERVER_VERSION)
	find . -name "vfabric-tc-server-*.tar.gz" | wc -l | grep -q 1
	touch $@

%.fetched: $(ARTIFACTS)/%*.tar.gz %.single
	cp $< .
	touch $@

%.single:
	find $(ARTIFACTS) -iname "$**.tar.gz" | wc -l | grep -q 1
