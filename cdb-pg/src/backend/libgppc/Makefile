
subdir = src/backend/libgppc
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# We don't support installcheck here, since installcheck in lib/ invokes
# the one in test/
all install uninstall clean:
	$(MAKE) -C lib $@
	$(MAKE) -C test $@
