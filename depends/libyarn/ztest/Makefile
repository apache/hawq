CC=g++ 
CPPFLAGS=-g -Wall -std=c++0x -I/Users/jcao/program/libyarn/include -Wno-write-strings
LDFLAGS=-L/Users/jcao/program/libyarn/lib -lyarn


bin = client_main c_client_main

all: $(bin)

test_main: test_main.cpp
	$(CC) $(CPPFLAGS) $(LDFLAGS) $^ -o $@
	
client_main: client_main.cpp
	$(CC) $(CPPFLAGS) $(LDFLAGS) $^ -o $@
	
		
clean:
	rm -rf $(bin) *.dSYM
