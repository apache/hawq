ROOT = ../..

COMPONENTS = \
			 pxf \
			 stack \

ARTIFACTS = $(ROOT)/artifacts
JENKINS_FETCH = $(ROOT)/tools/fetchBuild.sh

JENKINS_VIEW = view/PHD_2_x_DevBuild/job
JENKINS_stack = $(JENKINS_VIEW)/HudsonHD2_2_0HadoopStackBuild_Git

TARGET = $(COMPONENTS:%=%.fetched)

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	-rm -rf $(TARGET)
	-rm -f *~

stack.fetched:
	$(JENKINS_FETCH) $(JENKINS_stack) "PHD-[\.0-9]*-bin-dev-[0-9]*"
	touch $@

%.fetched: $(ARTIFACTS)/%*.tar.gz %.single
	mv $< .
	touch $@

%.single:
	find $(ARTIFACTS) -iname "$**.tar.gz" | wc -l | grep -q 1
