ROOT = ../..

COMPONENTS = \
			 pxf-api \
			 pxf-core \
			 pxf-hbase \
			 pxf-hdfs \
			 pxf-hive \
			 stack \

ARTIFACTS = $(ROOT)/artifacts
DISTSVR_FETCH = $(ROOT)/tools/fetchBuild.sh

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

%.fetched: $(ARTIFACTS)/%*.tar.gz %.single
	cp $< .
	touch $@

%.single:
	find $(ARTIFACTS) -iname "$**.tar.gz" | wc -l | grep -q 1
