ROOT = ../..

COMPONENTS = \
			 pxf \
			 hbase \
			 hive \
			 hadoop \
			 zookeeper \

ARTIFACTS = $(ROOT)/artifacts
JENKINS_FETCH = $(ROOT)/tools/fetchBuild.sh

JENKINS_VIEW = view/Ongoing-sprint-release-build/job
JENKINS_hadoop = $(JENKINS_VIEW)/HudsonHD2_0_1_0HadoopBuild_ongoing_sprint_release
JENKINS_hbase = $(JENKINS_VIEW)/HudsonHD2_0_1_0HBaseBuild_ongoing_sprint_release
JENKINS_zookeeper = $(JENKINS_VIEW)/HudsonHD2_0_1_0ZookeeperBuild_ongoing_sprint_release
JENKINS_hive = $(JENKINS_VIEW)/HudsonHD2_0_1_0HiveBuild_ongoing_sprint_release

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
