top_builddir=../../../../..
subdir=src/backend/utils/misc

TARGETS=guc

# Objects from backend, which don't need to be mocked but need to be linked.
guc_REAL_OBJS=\
    $(top_srcdir)/src/backend/access/hash/hashfunc.o \
    $(top_srcdir)/src/backend/access/heap/tuptoaster.o \
    $(top_srcdir)/src/backend/access/transam/filerepdefs.o \
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
    $(top_srcdir)/src/backend/utils/adt/arrayfuncs.o \
    $(top_srcdir)/src/backend/utils/adt/arrayutils.o \
    $(top_srcdir)/src/backend/utils/adt/datum.o \
    $(top_srcdir)/src/backend/utils/adt/like.o \
    $(top_srcdir)/src/backend/utils/adt/varlena.o \
    $(top_srcdir)/src/backend/utils/fmgr/fmgr.o \
    $(top_srcdir)/src/backend/utils/hash/dynahash.o \
    $(top_srcdir)/src/backend/utils/hash/hashfn.o \
    $(top_srcdir)/src/backend/utils/init/globals.o \
    $(top_srcdir)/src/port/strlcpy.o \
    $(top_srcdir)/src/port/path.o \
    $(top_srcdir)/src/port/pgstrcasecmp.o \
    $(top_srcdir)/src/port/qsort.o \
    $(top_srcdir)/src/port/thread.o \
    $(top_srcdir)/src/timezone/localtime.o \
    $(top_srcdir)/src/timezone/pgtz.o 

include ../../../../Makefile.mock
