#-------------------------------------------------------------------------
#
# Makefile for the managerment utilities
#
#-------------------------------------------------------------------------

subdir = tools/doc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SKIP_INSTALL=.gitignore|.p4ignore|.rcfile|Makefile
install:
	${INSTALL_SCRIPT} -d ${docdir}
	${INSTALL_SCRIPT} -d ${docdir}/cli_help
	for dirs in `find * -type d` ; do ${INSTALL_SCRIPT} -d ${docdir}/cli_help/$${dirs}; done
	for files in `find * -type f -maxdepth 0 | grep -v -E "${SKIP_INSTALL}"`; do ${INSTALL_DATA} $${files} ${docdir}/cli_help; done
	for files in `find gpconfigs -type f | grep -v -E "${SKIP_INSTALL}"`; do ${INSTALL_DATA} $${files} ${docdir}/cli_help/gpconfigs; done
