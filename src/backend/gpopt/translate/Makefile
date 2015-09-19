#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/gpopt/relcache/Makefile,v 1.15 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt/relcache
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

BLD_TOP = $(top_builddir)
include $(top_builddir)/build-utils/pivotal/releng.mk

include $(top_builddir)/src/backend/gpopt/gpopt.mk

OBJS =  CMappingColIdVar.o \
		CMappingVarColId.o \
		CMappingElementColIdParamId.o \
		CMappingColIdVarPlStmt.o \
		CMappingParamIdScalarId.o \
		CMappingColIdVarQuery.o \
		CMappingElementColIdTE.o \
		CStateDXLToQuery.o \
		CDXLTranslateContextBaseTable.o \
		CCTEListEntry.o \
		CContextDXLToPlStmt.o \
		CDXLTranslateContext.o \
		CQueryMutators.o \
		CTranslatorScalarToDXL.o \
		CTranslatorDXLToScalar.o \
		CTranslatorUtils.o \
		CTranslatorRelcacheToDXL.o \
		CTranslatorDXLToQuery.o \
		CTranslatorQueryToDXL.o \
		CTranslatorDXLToPlStmt.o \
		CTranslatorPlStmtToDXL.o

include $(top_srcdir)/src/backend/common.mk
