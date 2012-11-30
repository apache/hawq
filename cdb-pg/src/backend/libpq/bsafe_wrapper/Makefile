# Produces a shared library wrapping RSA BSAFE

subdir = src/backend/libpq/bsafe_wrapper
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

NAME= bsafe_wrapper
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

SYSTEM_NAME=$(shell uname -s)
PLATFORM_Darwin=PLATFORM_MACOSX
PLATFORM_Linux=PLATFORM_LINUX
PLATFORM_SunOS=PLATFORM_SOLARIS

RSA_PLATFORM=$(PLATFORM_$(SYSTEM_NAME))

# Flags for RSA BSAFE. Yes, this quantity of flags is totally gross
# but we cannot do anything about it.
override CFLAGS += -DCRYPTOCME_FIPSPROV -DR_OPT_FIPS140 \
	-DCRYPTOC_ME -DNO_PK_CERT -DCPU_X86 -DL_ENDIAN -DTERMIOS \
	-DRSA_PLATFORM=RSA_AMD_LINUX -DPTHREAD -D_REENTRANT -D$(RSA_PLATFORM) \
	-DFLAT_INC -DNO_PKCS12 -DNO_CRL -DNO_CERT -DNO_NTR -DNO_ERR -DNO_WTLS \
	-DNO_EX_DATA -DNO_PKI_SIZE -DNO_OCSP -DNO_PKCS7 -DNO_EXT -DNO_SOCK \
	-DNO_HDW_CERT -DNO_RC6 -DNO_X509CERT -DNO_CERTIMPL -DNO_CM -DNO_R_DIAG \
	-DNO_WTLSCERT -DNO_PROXY

ifneq ($(RSA_BSAFE_DY_LIBS),)
OBJS = rsa_bsafe_sha2.o
SHLIB_LINK += $(RSA_BSAFE_STATIC_LIBS) $(RSA_BSAFE_DY_LIBS)


all: all-lib

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib
backend_src = $(top_srcdir)/src/backend

# Mac OS needs dynamic_lookup for undefined symbols -- the symbols are in the
# postgres binary itself
ifeq ($(PORTNAME), darwin)
	override LINK.shared += -undefined dynamic_lookup
endif


install: install-lib

clean: clean-lib
	rm -f $(OBJS)
else

install clean all:

endif
