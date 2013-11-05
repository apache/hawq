ROOT = .

TARGET = allinall-singlecluster.tar.bz2

SINGLECLUSTER_ROOT = ../single-cluster
HAWQ_ROOT = ../../..

BUILDROOT = $(TARGET:%.tar.bz2=%)
SINGLECLUSTER_BUILDROOT = $(BUILDROOT)/phd
HAWQ_BUILDROOT = $(BUILDROOT)/hawq
HAWQ_BUILDROOT_CFG = $(HAWQ_BUILDROOT)/conf

HAWQ_VERSIONFILE = $(HAWQ_BUILDROOT)/version.txt

all: $(TARGET)

untar_hawq: $(HAWQ_ROOT)/hawq-*-x86_64.tar.gz
	mkdir -p $(HAWQ_BUILDROOT)
	tar -xvzf $< -C $(HAWQ_BUILDROOT)
	mv $(HAWQ_BUILDROOT)/hawq/* $(HAWQ_BUILDROOT)
	rm $(HAWQ_BUILDROOT)/hawq
	rmdir $(HAWQ_BUILDROOT)/hawq-*

hawq_version: $(HAWQ_BUILDROOT)/include/pg_config.h
	cat $< | grep HQ_VERSION | grep -o "\".\+\"" | tr -d \" | sed -e 's/^/Hawq version: /' >> $(HAWQ_VERSIONFILE)

hawq_conf: $(ROOT)/hawq_config
	mkdir -p $(HAWQ_BUILDROOT_CFG)
	cp $^ $(HAWQ_BUILDROOT_CFG)

untar_singlecluster: $(SINGLECLUSTER_ROOT)/singlecluster.tar.gz
	mkdir -p $(SINGLECLUSTER_BUILDROOT)
	tar -xvzf $< --strip-components 1 -C $(SINGLECLUSTER_BUILDROOT)

copy_files: $(ROOT)/README.txt $(ROOT)/init-cluster.sh
	cp $^ $(BUILDROOT)

$(BUILDROOT): untar_hawq hawq_version hawq_conf untar_singlecluster copy_files
	chmod -R +w $@

%.bz2: %
	bzip2 --best -c $< > $@

%.tar: %
	tar -cvf $@ $<

clean:
	-rm -rf $(BUILDROOT)
	-rm -f *~
	-rm -f $(TARGET)
