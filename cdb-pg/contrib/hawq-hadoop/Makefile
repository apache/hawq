# HAWQ-HADOOP Makefile

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/hawq-hadoop
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Maven subprojects
SUBPROJECTS = hawq-mapreduce-ao hawq-mapreduce-common hawq-mapreduce-tool
DEPENDENCIES = postgresql-9.2-1003-jdbc4.jar snakeyaml-1.12.jar
VERSION = 1.0.0
DEFAULTTARGET = hawq-mapreduce-tool/target/hawq-mapreduce-tool-$(VERSION).jar
JARDIR = hawq-mr-io

# Now the makefile target that do the work:
# The default target:
all: $(DEFAULTTARGET)

$(DEFAULTTARGET):
ifdef MAVEN
	$(MAVEN) package -DskipTests
else
	@$(missing) mvn $< $@
endif

# Make clean distclean
clean distclean:
ifdef MAVEN
	$(MAVEN) clean
else
	@$(missing) mvn $< $@
endif

# Make check
unittest-check check installcheck:
ifdef MAVEN
	$(MAVEN) test
else
	@$(missing) mvn $< $@
endif

# Make install
install: $(DEFAULTTARGET)
	@for subpro in $(SUBPROJECTS); do \
	echo "$(INSTALL_SHLIB) $$subpro/target/$$subpro-$(VERSION).jar '$(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro.jar'"; \
	$(INSTALL_SHLIB) $$subpro/target/$$subpro-$(VERSION).jar $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro.jar; \
	$(INSTALL_SHLIB) $$subpro/target/$$subpro-$(VERSION)-tests.jar $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro-tests.jar; \
	done
	echo "$(INSTALL_SHLIB) target/hawq-hadoop-$(VERSION)-javadoc.jar '$(DESTDIR)$(pkglibdir)/$(JARDIR)/hawq-hadoop-javadoc.jar'"
	$(INSTALL_SHLIB) target/hawq-hadoop-$(VERSION)-javadoc.jar $(DESTDIR)$(pkglibdir)/$(JARDIR)/hawq-hadoop-javadoc.jar
	@for dep in $(DEPENDENCIES); do \
	echo "$(INSTALL_SHLIB) hawq-mapreduce-tool/lib/$$dep '$(DESTDIR)$(pkglibdir)/$(JARDIR)/lib/$$dep'"; \
	$(INSTALL_SHLIB) hawq-mapreduce-tool/lib/$$dep $(DESTDIR)$(pkglibdir)/$(JARDIR)/lib/$$dep; \
	done

# Make uninstall
uninstall:
	@for subpro in $(SUBPROJECTS); do \
	rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro.jar; \
	rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/$$subpro-tests.jar; \
	done
	@rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/hawq-hadoop-javadoc.jar
	@for dep in $(DEPENDENCIES); do \
	rm -rf $(DESTDIR)$(pkglibdir)/$(JARDIR)/lib/$$dep; \
	done
