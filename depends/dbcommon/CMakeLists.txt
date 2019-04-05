CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(dbcommon)

SET(CMAKE_VERBOSE_MAKEFILE ON CACHE STRING "Verbose build." FORCE)

##############################################################################
# General CMake initialization
##############################################################################
SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
SET(DOXYFILE_PATH ${CMAKE_SOURCE_DIR}/docs)
SET(CMAKE_VERBOSE_MAKEFILE OFF CACHE STRING "Verbose build." FORCE)

IF(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    MESSAGE(FATAL_ERROR "cannot build the project in the source directory! Out-of-source build is enforced!")
ENDIF()

##############################################################################
# Import env, customer settings and utilities
##############################################################################
INCLUDE(Functions)
INCLUDE(Platform)
INCLUDE(Options)

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(test)

ADD_CUSTOM_TARGET(coverage
    COMMAND make resetcoverage
    COMMAND make -j8 unittest
    COMMAND make ucoverage
    COMMENT "Run all unit tests and get coverage...")
