# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# HAWQ-HADOOP Makefile

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/hawq-hadoop
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Maven subprojects
SUBPROJECTS = hawq-mapreduce-ao hawq-mapreduce-parquet hawq-mapreduce-common hawq-mapreduce-tool
DEPENDENCIES = postgresql-9.2-1003-jdbc4.jar snakeyaml-1.12.jar parquet-column-1.1.0.jar parquet-common-1.1.0.jar parquet-encoding-1.1.0.jar parquet-format-1.0.0.jar parquet-hadoop-1.1.0.jar snappy-java-1.1.0.jar
VERSION = 1.1.0
DEFAULTTARGET = hawq-mapreduce-tool/target/hawq-mapreduce-tool-$(VERSION).jar
JARDIR = hawq-mr-io

# Now the makefile target that do the work:
# The default target:
all: $(DEFAULTTARGET)

$(DEFAULTTARGET):
ifdef MAVEN
	$(MAVEN) package -DskipTests
else
	@$(missing) mvn $< $@
endif

# Make clean distclean
clean distclean:
ifdef MAVEN
	$(MAVEN) clean
else
	@$(missing) mvn $< $@
endif

# Make check
unittest-check check installcheck:
ifdef MAVEN
	$(MAVEN) test
else
	@$(missing) mvn $< $@
endif

# Make install
install: $(DEFAULTTARGET)
	@for subpro in $(SUBPROJECTS); do \
	echo "$(INSTALL_SHLIB) $$subpro/target/$$subpro-$(VERSION).jar '$(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro.jar'"; \
	$(INSTALL_SHLIB) $$subpro/target/$$subpro-$(VERSION).jar $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro.jar; \
	$(INSTALL_SHLIB) $$subpro/target/$$subpro-$(VERSION)-tests.jar $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro-tests.jar; \
	done
	echo "$(INSTALL_SHLIB) target/hawq-hadoop-$(VERSION)-javadoc.jar '$(DESTDIR)$(pkglibdir)/$(JARDIR)/hawq-hadoop-javadoc.jar'"
	$(INSTALL_SHLIB) target/hawq-hadoop-$(VERSION)-javadoc.jar $(DESTDIR)$(pkglibdir)/$(JARDIR)/hawq-hadoop-javadoc.jar
	@for dep in $(DEPENDENCIES); do \
	echo "$(INSTALL_SHLIB) hawq-mapreduce-tool/lib/$$dep '$(DESTDIR)$(pkglibdir)/$(JARDIR)/lib/$$dep'"; \
	$(INSTALL_SHLIB) hawq-mapreduce-tool/lib/$$dep $(DESTDIR)$(pkglibdir)/$(JARDIR)/lib/$$dep; \
	done

# Make uninstall
uninstall:
	@for subpro in $(SUBPROJECTS); do \
	rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro.jar; \
	rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro-tests.jar; \
	done
	@rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/hawq-hadoop-javadoc.jar
	@for dep in $(DEPENDENCIES); do \
	rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/lib/$$dep; \
	done
