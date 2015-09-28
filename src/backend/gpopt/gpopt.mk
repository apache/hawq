##-------------------------------------------------------------------------------------
##
## Copyright (C) 2012 EMC Corp.
##
## @doc: GP optimizer build 
##
## @author: solimm1
##
##-------------------------------------------------------------------------------------

UNAME = $(shell uname)
UNAME_P = $(shell uname -p)
ARCH_OS = GPOS_$(UNAME)
ARCH_CPU = GPOS_$(UNAME_P)

ifeq (Darwin, $(UNAME))
        LDSFX = dylib
else
        LDSFX = so
endif

BLD_TYPE=opt

GPOPT_flags = -g3 -DGPOS_DEBUG
ifeq "$(BLD_TYPE)" "opt"
	GPOPT_flags = -O3 -fno-omit-frame-pointer -g3
endif

ARCH_BIT = GPOS_64BIT
ifeq (Darwin, $(UNAME))
	ARCH_BIT = GPOS_32BIT
endif

ifeq ($(ARCH_BIT), GPOS_32BIT)
	ARCH_FLAGS = -m32
else
	ARCH_FLAGS = -m64
endif

BLD_FLAGS = $(ARCH_FLAGS) -D$(ARCH_BIT) -D$(ARCH_CPU) -D$(ARCH_OS) $(GPOPT_flags)
override CPPFLAGS := -fPIC $(CPPFLAGS)
override CPPFLAGS := $(BLD_FLAGS)  $(CPPFLAGS)
override CPPFLAGS := -DGPOS_VERSION=\"$(ORCA_DEPENDS_LIBGPOS_VER)\" $(CPPFLAGS)
override CPPFLAGS := -DGPOPT_VERSION=\"$(ORCA_DEPENDS_OPTIMIZER_VER)\" $(CPPFLAGS)
override CPPFLAGS := -DXERCES_VERSION=\"$(ORCA_DEPENDS_XERCES_VER)\" $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libgpos/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I $(ORCA_DEPENDS_DIR_INTER)/libgpdbcost/include $(CPPFLAGS)
