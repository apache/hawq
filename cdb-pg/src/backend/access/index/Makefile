#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/index
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/access/index/Makefile,v 1.14 2008/02/19 10:30:06 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/access/index
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = genam.o indexam.o catquery.o

include $(top_srcdir)/src/backend/common.mk

# Build a list of known source files with CaQL statements -- should
# rebuild catquery.c if any of these change.
# Note that caql.files "find" will discover CaQL statements in files
# that are *not* in this list, which is correct.  Amend this list as
# needed with latest contents of caql.files to improve Makefile
# dependencies for catquery.c regeneration
CAQL_CQL_SRCS := $(addprefix $(top_srcdir)/src/backend/,\
	access/common/tupdesc.c \
	access/heap/heapam.c \
	catalog/aclchk.c \
	catalog/dependency.c \
	catalog/gp_fastsequence.c \
	catalog/heap.c \
	catalog/index.c \
	catalog/namespace.c \
	catalog/pg_aggregate.c \
	catalog/pg_appendonly.c \
	catalog/pg_attribute_encoding.c \
	catalog/pg_compression.c \
	catalog/pg_constraint.c \
	catalog/pg_conversion.c \
	catalog/pg_depend.c \
	catalog/pg_extprotocol.c \
	catalog/pg_exttable.c \
	catalog/pg_namespace.c \
	catalog/pg_operator.c \
	catalog/pg_proc.c \
	catalog/pg_proc_callback.c \
	catalog/pg_shdepend.c \
	catalog/pg_type.c \
	catalog/toasting.c \
	cdb/cdbcat.c \
	cdb/cdbdisp.c \
	cdb/cdbpartition.c \
	cdb/cdbpersistentbuild.c \
	cdb/cdbsubselect.c \
	cdb/cdbutil.c \
	cdb/motion/tupser.c \
	commands/aggregatecmds.c \
	commands/analyze.c \
	commands/cluster.c \
	commands/comment.c \
	commands/conversioncmds.c \
	commands/dbcommands.c \
	commands/extprotocolcmds.c \
	commands/filespace.c \
	commands/foreigncmds.c \
	commands/functioncmds.c \
	commands/indexcmds.c \
	commands/opclasscmds.c \
	commands/operatorcmds.c \
	commands/proclang.c \
	commands/queue.c \
	commands/schemacmds.c \
	commands/sequence.c \
	commands/tablecmds.c \
	commands/tablespace.c \
	commands/trigger.c \
	commands/typecmds.c \
	commands/user.c \
	commands/vacuum.c \
	commands/variable.c \
	executor/functions.c \
	executor/nodeAgg.c \
	executor/nodeMergejoin.c \
	executor/nodeWindow.c \
	executor/spi.c \
	fts/fts.c \
	optimizer/plan/planpartition.c \
	optimizer/plan/planwindow.c \
	optimizer/util/clauses.c \
	optimizer/util/plancat.c \
	optimizer/util/predtest.c \
	parser/analyze.c \
	parser/parse_clause.c \
	parser/parse_coerce.c \
	parser/parse_func.c \
	parser/parse_node.c \
	parser/parse_oper.c \
	parser/parse_relation.c \
	parser/parse_type.c \
	parser/parse_utilcmd.c \
	rewrite/rewriteDefine.c \
	rewrite/rewriteRemove.c \
	rewrite/rewriteSupport.c \
	storage/large_object/inv_api.c \
	tcop/utility.c \
	utils/adt/acl.c \
	utils/adt/dbsize.c \
	utils/adt/format_type.c \
	utils/adt/regproc.c \
	utils/adt/ruleutils.c \
	utils/adt/selfuncs.c \
	utils/cache/lsyscache.c \
	utils/cache/relcache.c \
	utils/cache/typcache.c \
	utils/fmgr/fmgr.c \
	utils/fmgr/funcapi.c \
	utils/gp/segadmin.c \
	utils/init/miscinit.c \
	utils/misc/superuser.c \
	utils/resscheduler/resscheduler.c \
	utils/sort/tuplesort.c \
	)

# Generate catquery.c by discovering cql statements in src files

jversion := 4.2
caql_locktest_FLAGS := -lockcheck -readlock \
-holdlock \
-logquery \
-lwl=pg_type,pg_proc,pg_namespace,pg_operator,pg_opclass,pg_amop,pg_am,pg_amproc

# NOTE: to turn on query logging, do "make caql_logquery_FLAGS=-logquery"

caql.files: $(CAQL_CQL_SRCS) $(top_srcdir)/src/include/catalog/calico.pl 
	find $(top_srcdir)/ -name '*.c' | \
	xargs grep -l cql | grep -v catquery > caql.files 

catquery.o: catquery.c

catquery.c: caql.files $(top_srcdir)/src/include/catalog/calico.pl
	perl $(top_srcdir)/src/include/catalog/calico.pl \
		-meta $(top_srcdir)/../gpMgmt/bin/gppylib/data/$(jversion).json \
		$(caql_logquery_FLAGS) \
		-filemap caqlfilemap.json \
		-uniqdef uniqdef.json \
		-basedef basedef.json \
		-gperf gperf.init -infiles caql.files > catquery.c 
	# track the cql source files
	cat caql.files | xargs openssl dgst -md5 -hex | cat > caql.md5 

test: 
	echo $(caql_logquery_FLAGS) 
	echo $(caql_locktest_FLAGS) 

clean:
	rm -f SUBSYS.o $(OBJS) catquery.c caql.files gperf.init caqlfilemap.json uniqdef.json basedef.json

