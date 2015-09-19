#-------------------------------------------------------------------------
#
# Makefile for the managerment utilities
#
#-------------------------------------------------------------------------

subdir = tools/bin/gppylib
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SKIP_INSTALL=.gitignore|.p4ignore|.rcfile|Makefile|test/

install:
	${INSTALL_SCRIPT} -d ${libdir}/python/gppylib
	@for file in `find * -type f | grep -v -E "${SKIP_INSTALL}"`; \
		do \
			echo "install $${file} into ${libdir}/python/gppylib/$${file}" ; \
			${INSTALL_SCRIPT} $${file} ${libdir}/python/gppylib/$${file}; \
		done
	@for dirs in `find * -type d | grep -v test` ;\
		do \
			${INSTALL_SCRIPT} -d ${libdir}/python/gppylib/$${dirs}; \
			for file in `find $${dirs} -type f | grep -v -E "${SKIP_INSTALL}"`; do \
				echo "install $${file} into ${libdir}/python/gppylib/$${file}" ; \
				${INSTALL_SCRIPT} $${file} ${libdir}/python/gppylib/$${file}; \
			done \
		done
	