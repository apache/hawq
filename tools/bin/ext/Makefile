#-------------------------------------------------------------------------
#
# Makefile for the managerment utilities
#
#-------------------------------------------------------------------------

subdir = tools/bin/ext
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SKIP_INSTALL=.gitignore|.p4ignore|.rcfile|Makefile|test/

install:
	${INSTALL_SCRIPT} -d ${libdir}/python/
	@for file in `find * -type f | grep -v -E "${SKIP_INSTALL}"`; \
		do \
			echo "install $${file} into ${libdir}/python/$${file}" ; \
			${INSTALL_SCRIPT} $${file} ${libdir}/python/$${file}; \
		done
	@for dirs in `find * -type d | grep -v test` ;\
		do \
			${INSTALL_SCRIPT} -d ${libdir}/python/$${dirs}; \
			for file in `find $${dirs} -type f | grep -v -E "${SKIP_INSTALL}"`; do \
				echo "install $${file} into ${libdir}/python/$${file}" ; \
				${INSTALL_SCRIPT} $${file} ${libdir}/python/$${file}; \
			done \
		done
