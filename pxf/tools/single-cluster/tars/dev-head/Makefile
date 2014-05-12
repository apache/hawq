ROOT = ../..

COMPONENTS = \
			 pxf-api \
			 pxf-core \
			 pxf-hbase \
			 pxf-hdfs \
			 pxf-hive \
			 stack \
			 tcserver \

ARTIFACTS = $(ROOT)/artifacts
DISTSVR_FETCH = $(ROOT)/tools/fetchBuild.sh

TCSERVER_VERSION=2.9.5/vfabric-tc-server-standard-2.9.5.SR1.tar.gz
TCSERVER_URL_BASE=http://build-prod.dh.greenplum.com/releases/tcServer

DISTSVR_VIEW = latest
DISTSVR_stack = $(DISTSVR_VIEW)

TARGET = $(COMPONENTS:%=%.fetched)

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	-rm -rf $(TARGET)
	-rm -f *~

stack.fetched:
	$(DISTSVR_FETCH) $(DISTSVR_stack) "PHD-[\.0-9]*-bin-[0-9]*"
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
