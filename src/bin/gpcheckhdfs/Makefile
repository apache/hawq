
subdir = src/bin/gpcheckhdfs
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LIBS := -lprotobuf -lboost_thread -lboost_system -lboost_date_time -lstdc++ -lhdfs3 -lgsasl $(LIBS)

OBJS=gpcheckhdfs.o 

all: gpcheckhdfs

gpcheckhdfs:$(OBJS)  
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LIBS) -o $@$(X)

.PHONY: submake-backend
submake-backend:
	$(MAKE)

install: all installdirs
	$(INSTALL_PROGRAM) gpcheckhdfs$(X) '$(DESTDIR)$(bindir)'/gpcheckhdfs$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, gpcheckhdfs$(X))

clean distclean maintainer-clean:
	rm -f gpcheckhdfs$(X) $(OBJS) 

