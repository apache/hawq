#-------------------------------------------------------------------------
#
# Makefile for the managerment utilities
#
#-------------------------------------------------------------------------

subdir = tools/sbin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SKIP_INSTALL=.gitignore|.p4ignore|.rcfile|Makefile
install:
	${INSTALL_SCRIPT} -d $(DESTDIR)/${sbindir}
	for files in `find * -maxdepth 0 -type f | grep -x -v -E "${SKIP_INSTALL}"`; do ${INSTALL_SCRIPT} $${files} $(DESTDIR)/${sbindir}; done
