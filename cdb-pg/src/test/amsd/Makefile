#
# Makefile for CDB unit tests
#

subdir = src/test/amsd
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)
override LDLIBS := $(libpq) -lpgport $(LDLIBS)

OBJS = test-amsd.o 
PROGS = test-amsd

all: $(PROGS)

test-amsd: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LIBS) -o $@$(X)

clean:
	rm -f $(PROGS) $(OBJS)
