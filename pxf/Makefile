
ANT=/opt/releng/apache-ant/bin/ant

all: 
	./gradlew clean build
	
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
	$(ANT) -buildfile regression.xml
