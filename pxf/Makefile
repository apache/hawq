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
# protect the default target for this file from the targets in Makefile.global
default: all

ifneq "$(HD)" ""
    BUILD_PARAMS= -Dhd=$(HD)
else
    ifneq "$(PXF_HOME)" ""
        BUILD_PARAMS= -DdeployPath=$(PXF_HOME)
    else ifneq "$(GPHOME)" ""
        BUILD_PARAMS= -DdeployPath="$(GPHOME)/pxf"
    else
		@echo "Cannot invoke install without configuring either PXF_HOME or GPHOME"
    endif
endif

ifneq "$(LICENSE)" ""
    BUILD_PARAMS+= -Plicense="$(LICENSE)"
endif

ifneq "$(VENDOR)" ""
    BUILD_PARAMS+= -Pvendor="$(VENDOR)"
endif

ifneq "$(PXF_VERSION)" ""
    BUILD_PARAMS+= -Pversion="$(PXF_VERSION)"
endif

help:
	@echo 
	@echo"help it is then"
	@echo   "Possible targets"
	@echo	"  - all (clean, build, unittest, jar, tar, rpm)"
	@echo	"  -  -  HD=<phd|hdp> - set classpath to match hadoop distribution. default phd"
	@echo	"  -  -  LICENSE=<license info> - add license info to created RPMs"
	@echo	"  -  -  VENDOR=<vendor name> - add vendor name to created RPMs"
	@echo	"  - tomcat - builds tomcat rpm from downloaded tarball"
	@echo	"  -  -  LICENSE and VENDOR parameters can be used as well"
	@echo	"  - deploy - setup PXF along with tomcat in the configured deployPath"
	@echo	"  - doc - creates aggregate javadoc under docs"

all: 
	./gradlew clean release $(BUILD_PARAMS)
	
unittest:
	./gradlew test
	
jar:
	./gradlew jar $(BUILD_PARAMS)
	
tar:
	./gradlew tar $(BUILD_PARAMS)

rpm:
	./gradlew rpm $(BUILD_PARAMS)
	
clean:
	./gradlew clean

doc:
	./gradlew aggregateJavadoc 

.PHONY: tomcat
tomcat:
	./gradlew tomcatRpm $(BUILD_PARAMS)

install:
	./gradlew install $(BUILD_PARAMS)
