CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(libyarn)

SET(CMAKE_VERBOSE_MAKEFILE ON CACHE STRING "Verbose build." FORCE)
SET(CMAKE_FIND_ROOT_PATH "/Users/weikui/Documents/project/osx106_x86")

IF(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    MESSAGE(FATAL_ERROR "cannot build the project in the source directory! Out-of-source build is enforced!")
ENDIF()

SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
SET(DOXYFILE_PATH ${CMAKE_SOURCE_DIR}/docs)

INCLUDE(Platform)
INCLUDE(Functions)
INCLUDE(Options)
INCLUDE_DIRECTORIES(${GoogleTest_INCLUDE_DIR})

FIND_PACKAGE(LibXml2 REQUIRED)
FIND_PACKAGE(Protobuf REQUIRED)
FIND_PACKAGE(KERBEROS REQUIRED)
FIND_PACKAGE(GSasl REQUIRED)
FIND_PACKAGE(GoogleTest REQUIRED)

ADD_SUBDIRECTORY(mock)
ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(test)

ADD_CUSTOM_TARGET(doc
	COMMAND doxygen ${CMAKE_BINARY_DIR}/src/doxyfile
	WORKING_DIRECTORY ${DOXYFILE_PATH}
	COMMENT "Generate documents..."
)

ADD_CUSTOM_TARGET(style
	COMMAND astyle --style=attach --indent=spaces=4 --indent-preprocessor --break-blocks --pad-oper --pad-header --unpad-paren --delete-empty-lines --suffix=none --align-pointer=middle --lineend=linux --indent-col1-comments ${libyarn_SOURCES}
	COMMAND astyle --style=attach --indent=spaces=4 --indent-preprocessor --break-blocks --pad-oper --pad-header --unpad-paren --delete-empty-lines --suffix=none --align-pointer=middle --lineend=linux --indent-col1-comments ${unit_SOURCES}
	COMMAND astyle --style=attach --indent=spaces=4 --indent-preprocessor --break-blocks --pad-oper --pad-header --unpad-paren --delete-empty-lines --suffix=none --align-pointer=middle --lineend=linux --indent-col1-comments ${function_SOURCES}
	COMMAND astyle --style=attach --indent=spaces=4 --indent-preprocessor --break-blocks --pad-oper --pad-header --unpad-paren --delete-empty-lines --suffix=none --align-pointer=middle --lineend=linux --indent-col1-comments ${secure_SOURCES}
	WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
	COMMENT "format code style..."
)

