ROOT = .

ifndef $(RELEASE)
  RELEASE = dev-head
endif

BUILDVER=$(shell cat product.version)

BUILDNUM = $(BUILD_NUMBER)
ifeq ($(BUILDNUM),)
  BUILDNUM = $(shell whoami)
endif

TARGET = singlecluster.tar.gz

BUILDROOT = $(TARGET:%.tar.gz=%)
BINROOT = $(ROOT)/bin
TARSROOT = $(ROOT)/tars/$(RELEASE)
TEMPLATESROOT = $(ROOT)/templates
DOCSROOT = $(ROOT)/docs

VERSIONSFILE = $(BUILDROOT)/versions.txt

BINFILES = $(filter-out *~, $(wildcard $(BINROOT)/*))
TARFILES = $(subst $(TARSROOT)/,,$(wildcard $(TARSROOT)/*.tar.gz))
EXTRACTEDTARS = $(TARFILES:%.tar.gz=%.extracted)
TEMPLATES := $(shell find $(TEMPLATESROOT) -type f -not -iname "*~")

DIRT = \
	   *.extracted \
	   $(BUILDROOT) \
	   *~ \

.PHONY: all
all: clean $(TARGET)

.PHONY: clean
clean:
	-rm -rf $(TARGET)
	-rm -rf $(DIRT)

$(BUILDROOT): copy_binfiles create_versions_file extract_products copy_templates copy_docs
	chmod -R +w $(BUILDROOT)

.PHONY: copy_binfiles
copy_binfiles: $(BINFILES)
	mkdir -p $(BUILDROOT)/bin
	cp $^ $(BUILDROOT)/bin

.PHONY: create_versions_file
create_versions_file:
	echo build number: $(BUILDNUM) > $(VERSIONSFILE)
	echo release: $(RELEASE) >> $(VERSIONSFILE)
	echo single_cluster-$(BUILDVER) >> $(VERSIONSFILE)

.PHONY: extract_stack
extract_stack:
	find $(BUILDROOT) -iwholename "*PHD*.tar.gz" | grep "\/\(hadoop\|hbase\|zookeeper\|hive\)" | xargs -n1 tar -C $(BUILDROOT) -xzf
	find $(BUILDROOT) -type d -a -iname "PHD-*" | xargs rm -rf
	find $(BUILDROOT) -maxdepth 1 -type d | grep "\/\(hadoop\|hbase\|zookeeper\|hive\)" | xargs -n1 basename >> $(VERSIONSFILE)

.PHONY: merge_pxf
merge_pxf:
	mkdir $(BUILDROOT)/pxf
	for X in $(BUILDROOT)/pxf-*; do \
		mv $$X/* $(BUILDROOT)/pxf; \
		rmdir $$X; \
	done;

.PHONY: extract_products
extract_products: $(EXTRACTEDTARS) extract_stack merge_pxf
	for X in $(BUILDROOT)/*-[0-9]*; do \
		mv $$X `echo $$X | sed -e 's/^\($(BUILDROOT)\/[A-Za-z0-9]*\).*$$/\1/'`; \
	done;
	chmod -R +w $(BUILDROOT)

.PHONY: copy_templates
copy_templates: $(TEMPLATES)
	cp -r $(TEMPLATESROOT)/* $(BUILDROOT)/
	-find $(BUILDROOT) -iname "*~" | xargs rm -f

.PHONY: copy_docs
copy_docs: $(DOCSROOT)/README.txt
	cp $^ $(BUILDROOT)

.PHONY: refresh_tars
refresh_tars:
	make -C $(TARSROOT) clean all

%.gz: %
	gzip --best -c $< > $@

%.tar: %
	tar -cf $@ $<

%.extracted: $(TARSROOT)/%.tar.gz
	tar xzf $^ -C $(BUILDROOT)
	touch $@
	echo $* >> $(VERSIONSFILE)

