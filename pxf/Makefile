
ANT=/opt/releng/apache-ant/bin/ant

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
	$(ANT) -buildfile regression.xml
