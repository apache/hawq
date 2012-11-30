top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ANT=/opt/releng/apache-ant/bin/ant
all: gphdfs protocol_formatter

protocol_formatter:
	$(MAKE) -C src/protocol_formatter

gphdfs:
	$(ANT) clean
	$(ANT) dist -Dorg=org.apache.hadoop \
                -Dname=hadoop-core \
                -Drevision=0.20.1-gphdce-1.0.0.0 \
                -Dgphdgnet.version=gphd-1.0-gnet-1.0.0.1 \
                -Dgpgnet.src=1.0
	$(ANT) clean
	$(ANT) dist -Dorg=org.apache.hadoop \
                -Dname=hadoop-core \
                -Drevision=0.20.205.0-gphd-1.1.0.0 \
                -Dgphdgnet.version=gphd-1.1-gnet-1.1.0.0 \
                -Dgpgnet.src=1.1
	$(ANT) clean
	$(ANT) dist -Dorg=org.apache.hadoop \
                -Dname=hadoop-core \
                -Drevision=0.20.2 \
                -Dgphdgnet.version=gpmr-1.0-gnet-1.0.0.1 \
                -Dgpgnet.src=1.0
	$(ANT) clean
	$(ANT) dist -Dorg=org.apache.hadoop \
                -Dname=hadoop-core \
                -Drevision=0.20.2 \
                -Dgphdgnet.version=gpmr-1.2-gnet-1.0.0.1 \
                -Dgpgnet.src=1.0

	$(ANT) clean
	$(ANT) dist -Dorg=org.apache.hadoop \
                -Dname=hadoop-core \
                -Drevision=0.20.2-cdh3u2 \
                -Dgphdgnet.version=cdh3u2-gnet-1.1.0.0 \
                -Dgpgnet.src=1.1

gphdfsdoc:
	$(ANT) javadoc -Dorg=emc \
                   -Dname=hadoop-core-0.20.205.0-gphd \
                   -Drevision=1.1.0.0 \
                   -Dgphdgnet.version=gnet-1.1 \
                   -Dgpgnet.src=1.1
	$(TAR) -cf gnet-1.1-javadoc.tar gnet-1.1-javadoc
	$(ANT) javadoc -Dorg=org.apache.hadoop \
                   -Dname=hadoop-core \
                   -Drevision=0.20.2 \
                   -Dgphdgnet.version=gnet-1.0 \
                   -Dgpgnet.src=1.0
	$(TAR) -cf gnet-1.0-javadoc.tar gnet-1.0-javadoc

install: all installdirs gphdfsdoc
	$(MAKE) -C src/protocol_formatter install
	$(INSTALL_PROGRAM) $(top_srcdir)/src/bin/gphdfs/dist/*.jar '$(DESTDIR)$(libdir)/hadoop/'
	$(INSTALL_PROGRAM) $(top_srcdir)/src/bin/gphdfs/hadoop_env.sh$(X) '$(DESTDIR)$(libdir)/hadoop/hadoop_env.sh$(X)'
	$(INSTALL_DATA) $(top_srcdir)/src/bin/gphdfs/*-javadoc.tar '$(DESTDIR)${exec_prefix}/docs/javadoc'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
	$(MKDIR_P) '$(DESTDIR)$(libdir)/hadoop'
	$(MKDIR_P) '$(DESTDIR)${exec_prefix}/docs/javadoc'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/*.jar'
	rm -f '$(DESTDIR)$(bindir)/hadoop_env.sh$(X)'

clean:
	$(MAKE) -C src/protocol_formatter clean
	$(ANT) clean
	rm -rf *-javadoc
	rm -f *-javadoc.tar
