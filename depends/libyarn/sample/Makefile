CC=gcc
CPPFLAGS=-g -Wall -std=gnu99 -I../dist/include
LDFLAGS=-L../dist/lib -L/Users/yjin/greenplum-db-devel/lib -lyarn
#bin = c_client_main_profile
bin = c_client_main

all: $(bin)
		
clean:
	rm -rf $(bin) *.dSYM
