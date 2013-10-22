ROOT = .

TARGET = allinall-singlecluster.tar.bz2

BUILD_ROOT = $(TARGET:%.tar.bz2=%)
DEPS_ROOT = $(ROOT)/deps
SINGLECLUSTER_ROOT = $(BUILD_ROOT)/phd
HAWQ_ROOT = $(BUILD_ROOT)/hawq
HAWQCFG_ROOT = $(HAWQ_ROOT)/conf

HAWQ_VERSIONFILE = $(HAWQ_ROOT)/version.txt

all: $(TARGET)

untar_hawq: $(DEPS_ROOT)/hawq-*-x86_64.tar.gz
	mkdir -p $(HAWQ_ROOT)
	tar -xvzf $< -C $(HAWQ_ROOT)
	mv $(HAWQ_ROOT)/hawq/* $(HAWQ_ROOT)
	rm $(HAWQ_ROOT)/hawq
	rmdir $(HAWQ_ROOT)/hawq-*

hawq_version: $(HAWQ_ROOT)/include/pg_config.h
	cat $< | grep HQ_VERSION | grep -o "\".\+\"" | tr -d \" | sed -e 's/^/Hawq version: /' >> $(HAWQ_VERSIONFILE)

hawq_conf: $(ROOT)/hawq_config
	mkdir -p $(HAWQCFG_ROOT)
	cp $^ $(HAWQCFG_ROOT)

untar_singlecluster: $(DEPS_ROOT)/singlecluster.tar.gz
	mkdir -p $(SINGLECLUSTER_ROOT)
	tar -xvzf $< --strip-components 1 -C $(SINGLECLUSTER_ROOT)

copy_files: $(ROOT)/README.txt $(ROOT)/init-cluster.sh
	cp $^ $(BUILD_ROOT)

$(BUILD_ROOT): untar_hawq hawq_version hawq_conf untar_singlecluster copy_files
	chmod -R +w $@

%.bz2: %
	bzip2 --best -c $< > $@

%.tar: %
	tar -cvf $@ $<

clean:
	-rm -rf $(BUILD_ROOT)
	-rm -f *~
	-rm -f $(TARGET)
