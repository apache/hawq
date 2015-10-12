subdir=src/backend/access/external
top_builddir=../../../../..

#TODO: enable hd_work_mgr tests
#TARGETS=pxfuriparser hd_work_mgr pxfheaders ha_config pxffilters pxfmasterapi pxfanalyze
TARGETS=pxfuriparser pxfheaders ha_config pxffilters pxfmasterapi pxfanalyze

# Objects from backend, which don't need to be mocked but need to be linked.
COMMON_REAL_OBJS=\
    $(top_srcdir)/src/backend/access/hash/hashfunc.o \
    $(top_srcdir)/src/backend/bootstrap/bootparse.o \
    $(top_srcdir)/src/backend/lib/stringinfo.o \
    $(top_srcdir)/src/backend/nodes/bitmapset.o \
    $(top_srcdir)/src/backend/nodes/equalfuncs.o \
    $(top_srcdir)/src/backend/nodes/list.o \
    $(top_srcdir)/src/backend/parser/gram.o \
    $(top_srcdir)/src/backend/regex/regcomp.o \
    $(top_srcdir)/src/backend/regex/regerror.o \
    $(top_srcdir)/src/backend/regex/regexec.o \
    $(top_srcdir)/src/backend/regex/regfree.o \
    $(top_srcdir)/src/backend/storage/page/itemptr.o \
    $(top_srcdir)/src/backend/utils/adt/datum.o \
    $(top_srcdir)/src/backend/utils/adt/like.o \
    $(top_srcdir)/src/backend/utils/error/elog.o \
    $(top_srcdir)/src/backend/utils/hash/dynahash.o \
    $(top_srcdir)/src/backend/utils/hash/hashfn.o \
    $(top_srcdir)/src/backend/utils/init/globals.o \
    $(top_srcdir)/src/backend/utils/mb/mbutils.o \
    $(top_srcdir)/src/backend/utils/mb/wchar.o \
    $(top_srcdir)/src/backend/utils/misc/guc.o \
    $(top_srcdir)/src/port/exec.o \
    $(top_srcdir)/src/port/path.o \
    $(top_srcdir)/src/port/pgsleep.o \
    $(top_srcdir)/src/port/pgstrcasecmp.o \
    $(top_srcdir)/src/port/qsort.o \
    $(top_srcdir)/src/port/strlcpy.o \
    $(top_srcdir)/src/port/thread.o \
    $(top_srcdir)/src/timezone/localtime.o \
    $(top_srcdir)/src/timezone/pgtz.o \
    $(top_srcdir)/src/timezone/strftime.o \
    $(top_srcdir)/src/backend/utils/mmgr/redzone_handler.o \

# Objects from backend, which don't need to be mocked but need to be linked.
pxfuriparser_REAL_OBJS=$(COMMON_REAL_OBJS) \
    $(top_srcdir)/src/backend/utils/adt/formatting.o \
    $(top_srcdir)/src/backend/nodes/value.o \
    $(top_srcdir)/src/backend/utils/adt/numutils.o \
    $(top_srcdir)/src/backend/access/external/pxfutils.o
hd_work_mgr_REAL_OBJS=$(COMMON_REAL_OBJS) \
	$(top_srcdir)/src/backend/cdb/cdbutil.o \
	$(top_srcdir)/src/backend/access/external/pxfuriparser.o \
	$(top_srcdir)/src/backend/access/external/pxfutils.o

# numutils for pg_ltoa
pxfheaders_REAL_OBJS=$(COMMON_REAL_OBJS) \
	$(top_srcdir)/src/backend/utils/adt/numutils.o
ha_config_REAL_OBJS=$(COMMON_REAL_OBJS) \
	$(top_srcdir)/src/backend/utils/fmgrtab.o \
	$(top_srcdir)/src/backend/utils/adt/numutils.o
pxfmasterapi_REAL_OBJS=$(COMMON_REAL_OBJS) \
	$(top_srcdir)/src/backend/utils/fmgrtab.o	
pxffilters_REAL_OBJS=$(COMMON_REAL_OBJS) \
	$(top_srcdir)/src/backend/optimizer/util/clauses.o \
	$(top_srcdir)/src/backend/parser/parse_expr.o
pxfanalyze_REAL_OBJS=$(COMMON_REAL_OBJS) \
	$(top_srcdir)/src/backend/utils/adt/ruleutils.o \
	$(top_srcdir)/src/backend/parser/kwlookup.o \
	$(top_srcdir)/src/backend/utils/mb/encnames.o    

include ../../../../Makefile.mock

MOCK_LIBS += -ljson
