BLD_TOP=..
include $(BLD_TOP)/Makefile.global
include $(BLD_TOP)/releng/releng.mk
ANT=$(ANT_DIR)/bin/ant

ifneq "$(TEST)" ""
REGRESSION_TEST= -DtestClass=$(TEST) 
endif
ifneq "$(METHODS)" ""
REGRESSION_METHODS= -DtestMethods=$(METHODS)
endif


all: 
	$(ANT) all
	
unittest:
	$(ANT) run-tests
	
jar:
	$(ANT) jar
	
tar:
	$(ANT) tar

rpm:
	$(ANT) rpm
	
clean:
	$(ANT) clean

regressions:
	$(ANT) -buildfile regression.xml $(REGRESSION_TEST) $(REGRESSION_METHODS)
