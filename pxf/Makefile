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


all: 
	./gradlew clean release
	
unittest:
	./gradlew test
	
jar:
	./gradlew build 
	
tar:
	./gradlew distTar

rpm:
	./gradlew buildRpm
	
clean:
	./gradlew clean

regressions:
	cd $(REGRESSION_DIR) && ../$(ANT) -buildfile regression.xml $(REGRESSION_TEST) $(REGRESSION_METHODS)
