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
# HAWQ ranger-plugin makefile
subdir = ranger-plugin
top_builddir = ../
ranger_dir = $(prefix)/ranger
include Makefile.global

MVN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B -e"
##########################################################################
#
.PHONY: all install distclean clean uninstall

ifeq ($(enable_rps), yes)

all:
ifdef MAVEN
	$(MAVEN) $(MVN_OPTS) clean
	$(MAVEN) $(MVN_OPTS) package
else
	@$(missing) mvn $< $(MVN_OPTS) clean
	@$(missing) mvn $< $(MVN_OPTS) package
endif

install:
	@echo "Install ranger-plugin by copy corresponding files to HAWQ install dir."
	mkdir -p $(ranger_dir)/{bin,etc,lib,plugin-service}
	mkdir -p $(ranger_dir)/plugin-service/{bin,conf,lib,logs,temp,webapps,work}
	mkdir -p $(ranger_dir)/plugin-service/work/policycache
	cp $(top_builddir)$(subdir)/scripts/enable-ranger-plugin.sh  $(ranger_dir)/bin
	cp $(top_builddir)$(subdir)/scripts/rps.sh                   $(ranger_dir)/bin
	cp $(top_builddir)$(subdir)/scripts/catalina.sh              $(ranger_dir)/bin
	chmod -R 750 $(ranger_dir)/bin
	cp $(top_builddir)$(subdir)/conf/ranger-servicedef-hawq.json              $(ranger_dir)/etc
	cp $(top_builddir)$(subdir)/service/target/conf/ranger-hawq-security.xml  $(ranger_dir)/etc
	cp $(top_builddir)$(subdir)/service/target/conf/ranger-hawq-audit.xml     $(ranger_dir)/etc
	cp $(top_builddir)$(subdir)/service/target/conf/rps.properties            $(ranger_dir)/etc
	cp $(top_builddir)$(subdir)/service/target/conf/log4j.properties          $(ranger_dir)/etc
	cp $(top_builddir)$(subdir)/admin-plugin/target/ranger-plugin-admin-*.jar $(ranger_dir)/lib
	cp $(top_builddir)$(subdir)/admin-plugin/target/lib/postgresql-*.jar      $(ranger_dir)/lib
	cp $(top_builddir)$(subdir)/scripts/setenv.sh                                      $(ranger_dir)/plugin-service/bin
	cp $(top_builddir)$(subdir)/conf/server.xml                                        $(ranger_dir)/plugin-service/conf
	cp $(top_builddir)$(subdir)/conf/catalina.properties                               $(ranger_dir)/plugin-service/conf
	cp $(top_builddir)$(subdir)/service/target/ranger-plugin-service-*/WEB-INF/lib/*   $(ranger_dir)/plugin-service/lib/
	rm $(ranger_dir)/plugin-service/lib/jersey-*-1.9.jar
	cp $(top_builddir)$(subdir)/service/target/ranger-plugin-service-*.war             $(ranger_dir)/plugin-service/webapps/rps.war

uninstall:
	rm -rf $(ranger_dir)

clean:
ifdef MAVEN
	$(MAVEN) $(MVN_OPTS) clean
else
	@$(missing) mvn $< $@
endif

distclean: clean

else

all install distclean clean uninstall:

clean:
	-rm -rf admin-plugin/target
	-rm -rf service/target

distclean: clean

endif

rpm:
	@echo "Build rpm package for HAWQ ranger-plugin."
	./scripts/build_ranger_rpm.sh
