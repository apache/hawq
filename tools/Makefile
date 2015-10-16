#-------------------------------------------------------------------------
#
# Makefile for the managerment utilities
#
#-------------------------------------------------------------------------

subdir = tools
top_builddir = ..
include $(top_builddir)/src/Makefile.global

all:
	${MAKE} -C gpnetbench $@

install :
	${MAKE} -C bin $@
	${MAKE} -C sbin $@
	${MAKE} -C doc $@
	${MAKE} set_scripts_version
	${MAKE} -C gpnetbench $@

clean distclean:
	${MAKE} -C bin $@
	${MAKE} -C gpnetbench $@

#---------------------------------------------------------------------
# Set Version
#---------------------------------------------------------------------

SET_VERSION_SCRIPTS = \
	bin/gpaddmirrors \
	bin/gpbitmapreindex \
	bin/gpcheck \
	bin/gpcheckperf \
	bin/gpcrondump.py \
	bin/gpdbrestore \
	bin/gpdeletesystem \
	bin/gpexpand \
	bin/gpextract \
	bin/gpfilespace \
	bin/gpload.py \
	bin/gplogfilter \
	bin/gpmigrator \
	bin/gpmigrator_mirror \
	bin/gpmovemirrors \
	bin/gprebuildsystem \
	bin/gprecoverseg \
	bin/hawq \
	bin/gpscp \
	bin/gpsizecalc \
	bin/gpskew \
	bin/gpssh \
	bin/gpssh-exkeys \
	bin/gpstart \
	bin/gpstate \
	bin/gpstop \
	bin/gpsys1 \
	bin/lib/gpcheckcat \
	sbin/gpaddconfig.py \
	sbin/gpchangeuserpassword \
	sbin/gpcheck_hostdump \
	sbin/gpcleansegmentdir.py \
	sbin/gpfixuserlimts \
	sbin/gpgetstatususingtransition.py \
	sbin/gprepairmirrorseg.py \
	sbin/gpsegstart.py \
	sbin/gpsegstop.py \
	sbin/gpsegtoprimaryormirror.py \
	sbin/gpsetdbid.py \
	sbin/gpsuspend.py \
	sbin/gpupgrademirror.py \
	lib/python/gppylib/programs/clsAddMirrors.py \
	lib/python/gppylib/programs/clsHostCacheLookup.py \
	lib/python/gppylib/programs/clsInjectFault.py \
	lib/python/gppylib/programs/clsRecoverSegment.py \
	lib/python/gppylib/programs/clsSystemState.py \
	lib/python/gppylib/programs/gppkg.py \
	lib/python/gppylib/programs/kill.py \
	lib/python/gppylib/programs/verify.py \
	lib/python/gppylib/mainUtils.py \
	$(NULL)

set_scripts_version : 
	cd $(top_builddir) && \
	for file in $(SET_VERSION_SCRIPTS); do \
	    if [ -f $(prefix)/$${file} ]; then \
	        perl ./putversion $(prefix)/$${file} ; \
	    fi ; \
	done
