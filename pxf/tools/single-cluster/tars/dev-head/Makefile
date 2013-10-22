ROOT = ../..

COMPONENTS = \
			 pxf \
			 hbase \
			 hive \
			 hadoop \
			 zookeeper \

ARTIFACTS = $(ROOT)/artifacts
JENKINS_FETCH = $(ROOT)/tools/fetchBuild.sh

JENKINS_VIEW = view/Dev-build/job
JENKINS_hadoop = $(JENKINS_VIEW)/HudsonHD2_x_HadoopBuild
JENKINS_hbase = $(JENKINS_VIEW)/HudsonHD2_x_HBaseBuild
JENKINS_zookeeper = $(JENKINS_VIEW)/HudsonHD2_x_ZookeeperBuild
JENKINS_hive = $(JENKINS_VIEW)/HudsonHD2_x_HiveBuild

TARGET = $(COMPONENTS:%=%.fetched)

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	-rm -rf $(TARGET)
	-rm -f *~

%.fetched:
	$(JENKINS_FETCH) $(JENKINS_$*) $*
	touch $@

pxf.fetched: $(ARTIFACTS)/pxf*.tar.gz single_artifact
	mv $< .
	touch $@

.PHONY: single_artifact
single_artifact:
	find $(ARTIFACTS) -iname "pxf*.tar.gz" | wc -l | grep -q 1
