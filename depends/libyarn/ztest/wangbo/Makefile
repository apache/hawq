CC=g++ -g -Wall
CPPFLAGS=-std=c++0x -I/usr/local/libyarn/include 
LDFLAGS=-L/usr/local/libyarn/lib -lyarn



bin = client_main  

all: $(bin)
	
client_main: client_main.cpp
	$(CC) $(CPPFLAGS) $(LDFLAGS) $^ -o $@	
	
	
clean:
	rm -rf $(bin) *.dSYM
