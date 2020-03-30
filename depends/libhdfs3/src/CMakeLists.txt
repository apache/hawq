CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

SET(libhdfs3_VERSION_MAJOR 2)
SET(libhdfs3_VERSION_MINOR 2)
SET(libhdfs3_VERSION_PATCH 31)
SET(libhdfs3_VERSION_STRING "${libhdfs3_VERSION_MAJOR}.${libhdfs3_VERSION_MINOR}.${libhdfs3_VERSION_PATCH}")
SET(libhdfs3_VERSION_API 1)
SET(libhdfs3_ROOT_SOURCES_DIR ${CMAKE_SOURCE_DIR}/src)
SET(libhdfs3_COMMON_SOURCES_DIR ${libhdfs3_ROOT_SOURCES_DIR}/common)

IF(ENABLE_DEBUG)
    SET(libhdfs3_VERSION_STRING "${libhdfs3_VERSION_STRING}d")
ENDIF(ENABLE_DEBUG)

CONFIGURE_FILE(platform.h.in platform.h)
CONFIGURE_FILE(doxyfile.in doxyfile)

AUTO_SOURCES(files "*.cpp" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libhdfs3_SOURCES ${files})

AUTO_SOURCES(files "*.cc" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libhdfs3_SOURCES ${files})

AUTO_SOURCES(files "*.c" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libhdfs3_SOURCES ${files})

AUTO_SOURCES(files "*.h" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libhdfs3_SOURCES ${files})

AUTO_SOURCES(libhdfs3_PROTO_FILES "proto/*.proto" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
SET(libhdfs3_PROTO_FILES ${libhdfs3_PROTO_FILES} PARENT_SCOPE)

PROTOBUF_GENERATE_CPP(libhdfs3_PROTO_SOURCES libhdfs3_PROTO_HEADERS ${libhdfs3_PROTO_FILES})

SET(HEADER 
    client/BlockLocation.h
    client/DirectoryIterator.h
    client/FileStatus.h
    client/FileSystem.h
    client/FileSystemStats.h
    client/hdfs.h
    client/InputStream.h
    client/OutputStream.h
    client/Permission.h
    common/Exception.h
    common/XmlConfig.h)

ADD_LIBRARY(libhdfs3-static STATIC ${libhdfs3_SOURCES} ${libhdfs3_PROTO_SOURCES} ${libhdfs3_PROTO_HEADERS})
ADD_LIBRARY(libhdfs3-shared SHARED ${libhdfs3_SOURCES} ${libhdfs3_PROTO_SOURCES} ${libhdfs3_PROTO_HEADERS})

ADD_CUSTOM_COMMAND(
    TARGET libhdfs3-shared libhdfs3-static
    PRE_BUILD
    COMMAND echo ${libhdfs3_VERSION_STRING} > version
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

TARGET_LINK_LIBRARIES(libhdfs3-static pthread)
TARGET_LINK_LIBRARIES(libhdfs3-shared pthread)
       
TARGET_LINK_LIBRARIES(libhdfs3-static Boost::boost)
TARGET_LINK_LIBRARIES(libhdfs3-shared Boost::boost)
IF(NEED_BOOST)
    TARGET_LINK_LIBRARIES(libhdfs3-static Boost::thread)
    TARGET_LINK_LIBRARIES(libhdfs3-static Boost::chrono)
    TARGET_LINK_LIBRARIES(libhdfs3-static Boost::system)
    TARGET_LINK_LIBRARIES(libhdfs3-static Boost::atomic)
    TARGET_LINK_LIBRARIES(libhdfs3-static Boost::iostreams)
    TARGET_LINK_LIBRARIES(libhdfs3-shared Boost::thread)
    TARGET_LINK_LIBRARIES(libhdfs3-shared Boost::chrono)
    TARGET_LINK_LIBRARIES(libhdfs3-shared Boost::system)
    TARGET_LINK_LIBRARIES(libhdfs3-shared Boost::atomic)
    TARGET_LINK_LIBRARIES(libhdfs3-shared Boost::iostreams)
ENDIF(NEED_BOOST)

IF(NEED_GCCEH)
    TARGET_LINK_LIBRARIES(libhdfs3-static gcc_eh)
    TARGET_LINK_LIBRARIES(libhdfs3-shared gcc_eh)
ENDIF(NEED_GCCEH)

IF(OS_LINUX)
	TARGET_LINK_LIBRARIES(libhdfs3-static LibUUID::LibUUID)
    TARGET_LINK_LIBRARIES(libhdfs3-shared LibUUID::LibUUID)
ENDIF(OS_LINUX)

TARGET_INCLUDE_DIRECTORIES(libhdfs3-static PUBLIC ${libhdfs3_ROOT_SOURCES_DIR})
TARGET_INCLUDE_DIRECTORIES(libhdfs3-static PUBLIC ${libhdfs3_COMMON_SOURCES_DIR})
TARGET_INCLUDE_DIRECTORIES(libhdfs3-static PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
TARGET_INCLUDE_DIRECTORIES(libhdfs3-static PUBLIC ${CMAKE_SOURCE_DIR}/mock)

TARGET_INCLUDE_DIRECTORIES(libhdfs3-shared PUBLIC ${libhdfs3_ROOT_SOURCES_DIR})
TARGET_INCLUDE_DIRECTORIES(libhdfs3-shared PUBLIC ${libhdfs3_COMMON_SOURCES_DIR})
TARGET_INCLUDE_DIRECTORIES(libhdfs3-shared PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
TARGET_INCLUDE_DIRECTORIES(libhdfs3-shared PUBLIC ${CMAKE_SOURCE_DIR}/mock)

TARGET_LINK_LIBRARIES(libhdfs3-static protobuf::libprotobuf)
TARGET_LINK_LIBRARIES(libhdfs3-static LibXml2::LibXml2)
TARGET_LINK_LIBRARIES(libhdfs3-static KERBEROS::KERBEROS)
TARGET_LINK_LIBRARIES(libhdfs3-static GSASL::GSASL)
TARGET_LINK_LIBRARIES(libhdfs3-static SSL::SSL)
TARGET_LINK_LIBRARIES(libhdfs3-static Curl::Curl)

TARGET_LINK_LIBRARIES(libhdfs3-shared protobuf::libprotobuf)
TARGET_LINK_LIBRARIES(libhdfs3-shared LibXml2::LibXml2)
TARGET_LINK_LIBRARIES(libhdfs3-shared KERBEROS::KERBEROS)
TARGET_LINK_LIBRARIES(libhdfs3-shared GSASL::GSASL)
TARGET_LINK_LIBRARIES(libhdfs3-shared SSL::SSL)
TARGET_LINK_LIBRARIES(libhdfs3-shared Curl::Curl)

SET_TARGET_PROPERTIES(libhdfs3-static PROPERTIES OUTPUT_NAME "hdfs3")
SET_TARGET_PROPERTIES(libhdfs3-shared PROPERTIES OUTPUT_NAME "hdfs3")

SET_TARGET_PROPERTIES(libhdfs3-shared PROPERTIES 
    VERSION ${libhdfs3_VERSION_MAJOR}.${libhdfs3_VERSION_MINOR}.${libhdfs3_VERSION_PATCH} 
    SOVERSION ${libhdfs3_VERSION_API})

INSTALL(TARGETS libhdfs3-static libhdfs3-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
INSTALL(FILES ${HEADER} DESTINATION include/hdfs)
INSTALL(FILES libhdfs3.pc DESTINATION lib/pkgconfig)
            
SET(libhdfs3_SOURCES ${libhdfs3_SOURCES} PARENT_SCOPE)
SET(libhdfs3_PLATFORM_HEADER_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
SET(libhdfs3_ROOT_SOURCES_DIR ${libhdfs3_ROOT_SOURCES_DIR} PARENT_SCOPE)
SET(libhdfs3_COMMON_SOURCES_DIR ${libhdfs3_COMMON_SOURCES_DIR} PARENT_SCOPE)
SET(libhdfs3_VERSION_STRING ${libhdfs3_VERSION_STRING} PARENT_SCOPE)
