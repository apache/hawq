# protect the default target for this file from the targets in Makefile.global
default: all

BLD_TOP=..
include $(BLD_TOP)/Makefile.global
include $(BLD_TOP)/releng/releng.mk
ANT=$(ANT_DIR)/bin/ant

REGRESSION_DIR=regression

ifneq "$(TEST)" ""
REGRESSION_TEST= -DtestClass=$(TEST) 
endif
ifneq "$(METHODS)" ""
REGRESSION_METHODS= -DtestMethods=$(METHODS)
endif

help:
	@echo 
	@echo	"help it is then"
	@echo	"Possible targets"
	@echo	"  - all (clean, build, unittest, jar, tar, rpm)"
	@echo	"  - regressions - run all regression tests"
	@echo	"  -  -  TEST=<testClass> - run regressions only from testClass"
	@echo	"  -  -  TEST=<testClass> METHODS=<method1,method2> - execute method1 and method2 from testClass"
	@echo	"  - regression-resources - compile regression-test.jar into $$GPHD_ROOT"

all: 
	./gradlew clean release
	
unittest:
	./gradlew test
	
jar:
	./gradlew jar 
	
tar:
	./gradlew tar

rpm:
	./gradlew rpm
	
clean:
	./gradlew clean

regressions:
	cd $(REGRESSION_DIR) && ../$(ANT) -buildfile regression.xml $(REGRESSION_TEST) $(REGRESSION_METHODS)

regression-resources:
	if [ ! -d $(GPHD_ROOT)/pxf ]; then echo GPHD_ROOT undefined; exit 1; fi
	cd $(REGRESSION_DIR) && ../$(ANT) -buildfile regression.xml jar-resources
	@echo
	@echo NOTE: regression resources compiled into $(GPHD_ROOT)/pxf
	@echo
