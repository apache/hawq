#
# Makefile for utils/stat/libgpla
#
# A wrapper for CLAPACK and BLAS
#

subdir = src/backend/utils/stat/libgpla
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global


# Need global Makefile to determine where the libraries are
BLD_TOP=$(top_builddir)/..
include $(BLD_TOP)/Makefile.global

# Order is important: libblas contains symbols for the other two
LAPACK_BLAS_LIBS=$(BLD_THIRDPARTY_LIB_DIR)/libblas.a \
                 $(BLD_THIRDPARTY_LIB_DIR)/liblapack.a \
                 $(BLD_THIRDPARTY_LIB_DIR)/libf2c.a -lblas -llapack -lf2c

NAME=gpla
SO_MAJOR_VERSION=1
SO_MINOR_VERSION=0

# We need to filter out everything we don't want linked in right now so that we
# can extract the location of our libraries 

LIBS := $(filter-out -lesmtp -lnetsnmp -lpgport -lz -lcurl -lbz2 -lpam -lssl -lcrypto -lkrb5 -lcom_err -lz -ledit -lm -pthread -pthreads  , $(LIBS))

OBJS = gpla.o
SHLIB_LINK += $(LAPACK_BLAS_LIBS)

include $(top_srcdir)/src/Makefile.shlib

all: all-lib

install: install-lib

clean: clean-lib
	rm -f $(OBJS)
