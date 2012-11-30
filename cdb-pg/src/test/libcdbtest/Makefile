# Makefile copied from dblink

subdir = src/test/libcdbtest
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global


# shared library parameters
NAME= cdbtest
SO_MAJOR_VERSION= 2
SO_MINOR_VERSION= 0

override CPPFLAGS :=  -DFRONTEND -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/port -I$(libpq_srcdir)
override CFLAGS += $(PTHREAD_CFLAGS)

# Need to recomple any libpgport object files
LIBS := $(patsubst -lpgport,, $(LIBS))

OBJS=	chunkfuncs.o cdbexecplan.o cdbshowalias.o ipcfuncs.o hashapi_access.o \
		cdbheaptest.o cdboid.o \
	$(filter crypt.o getaddrinfo.o inet_aton.o open.o snprintf.o strerror.o, $(LIBOBJS))

# Link against the Postgres backend and libpq.
SHLIB_LINK = $(BE_DLLLIBS) $(libpq)


all: $(top_builddir)/src/port/pg_config_paths.h all-lib

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib
backend_src = $(top_srcdir)/src/backend


install: all installdirs install-lib

installdirs:
	$(mkinstalldirs) $(DESTDIR)$(libdir)

uninstall: uninstall-lib

clean distclean: clean-lib
	rm -f $(OBJS)

maintainer-clean: distclean

