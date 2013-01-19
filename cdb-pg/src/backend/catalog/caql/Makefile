
subdir = src/backend/catalog/caql
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = caqlanalyze.o gram.o

FLEXFLAGS = -CF

include $(top_srcdir)/src/backend/common.mk

ifneq ($(findstring -logquery, $(caql_logquery_FLAGS)),)
override CPPFLAGS := $(CPPFLAGS) -DCAQL_LOGQUERY
endif

gram.o: $(srcdir)/scan.c

$(srcdir)/gram.h: $(srcdir)/gram.c ;

$(srcdir)/gram.c: gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

$(srcdir)/scan.c: scan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	# This is due to flex bug around unused variable yyg in <= 2.5.35
	sed -i'' -e 's/^.* depending upon options\. \*\/$$//' $@
else
	@$(missing) flex $< $@
endif

clean:
	rm -f gram.c gram.h scan.c
