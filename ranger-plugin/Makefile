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
include Makefile.global

MVN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B -e"
##########################################################################
#
.PHONY: all install distclean clean uninstall

rpm:
	@echo "Build rpm package for HAWQ ranger-plugin."
	./scripts/build_ranger_rpm.sh

ifeq ($(enable_rps), yes)

all:
ifdef MAVEN
	$(MAVEN) $(MVN_OPTS) clean
	$(MAVEN) $(MVN_OPTS) package
	$(MAVEN) $(MVN_OPTS) install
else
	@$(missing) mvn $< $(MVN_OPTS) clean
	@$(missing) mvn $< $(MVN_OPTS) package
	@$(missing) mvn $< $(MVN_OPTS) install
endif

install:
	@echo "Install ranger-plugin by copy ranger-plugin to HAWQ install dir."
	@echo "install `find $(top_builddir)$(subdir)/target -name ranger` into ${prefix}/ranger"
	cp -r `find $(top_builddir)$(subdir)/target -name ranger` ${prefix}/
	chmod -R 750 $(prefix)/ranger/bin

uninstall:
	rm -rf $(prefix)/ranger

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
	rm -rf target/rpm/*

distclean: clean

endif
