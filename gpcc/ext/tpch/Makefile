#
# $Id$
#
# Revision History
# ===================
# $Log: Makefile,v $
# Revision 1.3  2007/10/25 05:44:24  cktan
# more cleanup
#
# Revision 1.2  2007/10/24 21:22:38  cktan
# make dbgen only, no direct load
#
# Revision 1.1  2007/10/24 20:26:26  cktan
# new
#
# Revision 1.7  2007/09/12 21:55:05  cmcdevitt
# fix a lot of incorrect format specifiers, and fix some 64-bit issues
#
# Revision 1.6  2007/03/28 16:58:25  tkordas
# Change MPPHOME->GPHOME
#
# Revision 1.5  2007/03/21 22:01:37  swidom
# Added change_to_gp_prefix and change_to_mpp_prefix tasks to Makefile
#
# Revision 1.4  2006/04/25 18:55:14  aparashar
# Changed the Makefile so that dbgen can produce files greater than 2GB.
#
# Revision 1.3  2006/04/20 00:20:21  cmcdevitt
# fix extra delimiter at end of line
#
# Revision 1.2  2006/04/19 16:42:50  tkordas
# eliminate arch=pentium4 so that things compile on stinger4 (and the Mac).
#
# Revision 1.1  2006/04/18 23:50:51  cmcdevitt
# add a Makefile instead of makefile.suite to reduce confusion
#
# Revision 1.2  2006/04/18 23:48:40  cmcdevitt
# Fix referece to my local files
#
# Revision 1.1  2006/04/18 23:17:18  cmcdevitt
# Add TPC-H code.. dbgen, qgen, and tpchdriver
#
# Revision 1.2  2005/01/03 20:08:58  jms
# change line terminations
#
# Revision 1.1.1.1  2004/11/24 23:31:47  jms
# re-establish external server
#
# Revision 1.5  2004/03/26 20:39:23  jms
# add tpch tag to release files
#
# Revision 1.4  2004/03/16 14:45:57  jms
# correct release target in makefile
#
# Revision 1.3  2004/03/02 20:49:01  jms
# simplify distributions, add Windows IDE files
# releases should use make release from now on
#
# Revision 1.2  2004/02/18 14:05:53  jms
# porting changes for LINUX and 64 bit RNG
#
# Revision 1.1.1.1  2003/04/03 18:54:21  jms
# recreation after CVS crash
#
# Revision 1.1.1.1  2003/04/03 18:54:21  jms
# initial checkin
#
#
#
################
## CHANGE NAME OF ANSI COMPILER HERE
################
CC      = gcc -O3 -funroll-loops -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE=1 -Wall
# Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)
#                                  SQLSERVER, SYBASE
# Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, 
#                                  SGI, SUN, U2200, VMS
# Current values for WORKLOAD are:  TPCH, TPCR
DATABASE= POSTGRES
ifeq "$(shell uname -s)" "SunOS"
MACHINE = SUN
else
MACHINE = LINUX
endif
WORKLOAD = TPCH
#
# add -EDTERABYTE if orderkey will execeed 32 bits (SF >= 300)
# and make the appropriate change in gen_schema() of runit.sh
CFLAGS	= -g -DDBNAME=\"dss\" -D$(MACHINE) -D$(DATABASE) -D$(WORKLOAD) -I$(GPHOME)/include -DEOL_HANDLING
LDFLAGS = -g -L$(GPHOME)/lib
# The OBJ,EXE and LIB macros will need to be changed for compilation under
#  Windows NT
OBJ     = .o
EXE     =
LIBS    = -lm 
#
# NO CHANGES SHOULD BE NECESSARY BELOW THIS LINE
###############
TREE_ROOT=/tmp/tree
#
PROG1 = dbgen$(EXE)
PROGS = $(PROG1) 
#
HDR1 = dss.h rnd.h config.h dsstypes.h shared.h bcd2.h rng64.h cdbhash.h
HDR2 = tpcd.h permute.h
HDR3 = config.h
HDR  = $(HDR1) $(HDR2)
#
SRC1 = build.c driver.c bm_utils.c rnd.c print.c load_stub.c bcd2.c \
	speed_seed.c text.c permute.c rng64.c cdbhash.c
SRC2 = qgen.c varsub.c 
SRC  = $(SRC1) $(SRC2) $(SRC3)
#
OBJ1 = build$(OBJ) driver$(OBJ) bm_utils$(OBJ) rnd$(OBJ) print$(OBJ) \
	load_stub$(OBJ) bcd2$(OBJ) speed_seed$(OBJ) text$(OBJ) permute$(OBJ) \
	rng64$(OBJ) cdbhash$(OBJ)
OBJ2 = build$(OBJ) bm_utils$(OBJ) qgen$(OBJ) rnd$(OBJ) varsub$(OBJ) \
	text$(OBJ) bcd2$(OBJ) permute$(OBJ) speed_seed$(OBJ) rng64$(OBJ) cdbhash$(OBJ)
OBJ3 = tpchdriver$(OBJ) vsub$(OBJ) build$(OBJ) bm_utils$(OBJ) rnd$(OBJ) text$(OBJ) \
	bcd2$(OBJ) permute$(OBJ) speed_seed$(OBJ) rng64$(OBJ) cdbhash$(OBJ)
OBJS = $(OBJ1) $(OBJ2) $(OBJ3)
#
SETS = dists.dss 
DOC=README HISTORY PORTING.NOTES BUGS
DDL  = dss.ddl dss.ri
WINDOWS_IDE = tpch.dsw dbgen.dsp
OTHER=makefile.suite $(SETS) $(DDL) $(WINDOWS_IDE)
# case is *important* in TEST_RES
TEST_RES = O.res L.res c.res s.res P.res S.res n.res r.res
#
DBGENSRC=$(SRC1) $(HDR1) $(OTHER) $(DOC) $(SRC2) $(HDR2) $(SRC3)
FQD=queries/1.sql queries/2.sql queries/3.sql queries/4.sql queries/5.sql queries/6.sql queries/7.sql \
	queries/8.sql queries/9.sql queries/10.sql queries/11.sql queries/12.sql queries/13.sql \
	queries/14.sql queries/15.sql queries/16.sql queries/17.sql queries/18.sql queries/19.sql queries/20.sql \
	queries/21.sql queries/22.sql
VARIANTS= variants/8a.sql variants/12a.sql variants/13a.sql variants/14a.sql variants/15a.sql 
ANS   = answers/1.ans answers/2.ans answers/3.ans answers/4.ans answers/5.ans answers/6.ans answers/7.ans answers/8.ans \
	answers/9.ans answers/10.ans answers/11.ans answers/12.ans answers/13.ans answers/14.ans answers/15.ans \
	answers/16.ans answers/17.ans answers/18.ans answers/19.ans answers/20.ans answers/21.ans answers/22.ans
QSRC  = $(FQD) $(VARIANTS) $(ANS)
ALLSRC=$(DBGENSRC) $(QSRC)
TREE_DOC=tree.readme tree.changes appendix.readme appendix.version answers.readme queries.readme variants.readme
JUNK  = 
#
all: $(PROGS)
$(PROG1): $(OBJ1) $(SETS) 
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJ1) $(LIBS)
$(PROG2): permute.h $(OBJ2) 
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJ2) $(LIBS)
$(PROG3): $(OBJ3)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJ3) $(LIBS)
clean:
	rm -f $(PROGS) $(OBJS) $(JUNK)
lint:
	lint $(CFLAGS) -u -x -wO -Ma -p $(SRC1)
	lint $(CFLAGS) -u -x -wO -Ma -p $(SRC2)
	lint $(CFLAGS) -u -x -wO -Ma -p $(SRC3)


change_to_gp_prefix:
	perl -pi -e "s/mpp_session_role/gp_session_role/" load_stub.c

change_to_mpp_prefix:
	perl -pi -e "s/gp_session_role/mgp_session_role/" load_stub.c

tar: $(ALLSRC) 
	tar cvzhf tpch_`date '+%Y%m%d'`.tar.gz $(ALLSRC) 
zip: $(ALLSRC)
	zip tpch_`date '+%Y%m%d'`.zip $(ALLSRC)
release:
	make -f makefile.suite tar
	make -f makefile.suite zip
	( cd tests; sh test_list.sh `date '+%Y%m%d'` )
rnd$(OBJ): rnd.h
$(OBJ1): $(HDR1)
$(OBJ2): dss.h tpcd.h config.h rng64.h
$(OBJ3): config.h
