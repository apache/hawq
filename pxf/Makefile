# protect the default target for this file from the targets in Makefile.global
default: all

ifneq "$(HD)" ""
BUILD_PARAMS= -Dhd=$(HD)
endif

help:
	@echo 
	@echo	"help it is then"
	@echo	"Possible targets"
	@echo	"  - all (clean, build, unittest, jar, tar, rpm)"
	@echo	"  -  -  HD=<phd|hdp> - set classpath to match hadoop distribution. default phd"
	@echo	"  - tomcat - builds tomcat rpm from downloaded tarball"

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

.PHONY: tomcat
tomcat:
	./gradlew tomcatRpm

