#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/gpopt/translate/Makefile,v 1.15 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt/translate
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

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
		CTranslatorQueryToDXL.o \
		CTranslatorDXLToPlStmt.o 

include $(top_srcdir)/src/backend/common.mk
