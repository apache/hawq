CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(dbcommon)

FIND_PACKAGE(Glog REQUIRED)

SET(dbcommon_VERSION_MAJOR 0)
SET(dbcommon_VERSION_MINOR 1)
SET(dbcommon_VERSION_PATCH 0)
SET(dbcommon_VERSION_API 1)
set(CMAKE_MACOSX_RPATH 1)

SET(dbcommon_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
SET(dbcommon_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dbcommon)
# cogapp generation
find_package(Cogapp)
SET(CODE_TEMPLATE_HEADERS
    ${dbcommon_SRC_DIR}/function/arith-cmp-func.h 
    ${dbcommon_SRC_DIR}/function/typecast-func.h 
    ${dbcommon_SRC_DIR}/function/func-kind.h 
)

SET(CODE_TEMPLATE_SOURCES
    ${dbcommon_SRC_DIR}/function/arith-func.cc
    ${dbcommon_SRC_DIR}/function/cmp-func.cc 
    ${dbcommon_SRC_DIR}/function/typecast-func.cc 
    ${dbcommon_SRC_DIR}/function/func.cc 
    ${dbcommon_SRC_DIR}/type/type-util.cc 
)

COGAPP_GENERATE(TEMPLATE_GENERATED_HEADERS ${CODE_TEMPLATE_HEADERS})
COGAPP_GENERATE(TEMPLATE_GENERATED_SOURCES ${CODE_TEMPLATE_SOURCES})

set(thrift_SRC_DIR ${CMAKE_BINARY_DIR}/codegen/src/dbcommon/thrift)
set(thrift_HDRS
    ${thrift_SRC_DIR}/fb303_types.h
    ${thrift_SRC_DIR}/fb303_constants.h
    ${thrift_SRC_DIR}/FacebookService.h
    ${thrift_SRC_DIR}/hive_metastore_types.h
    ${thrift_SRC_DIR}/hive_metastore_constants.h
    ${thrift_SRC_DIR}/ThriftHiveMetastore.h)
set(thrift_SRCS
    ${thrift_SRC_DIR}/fb303_types.cpp
    ${thrift_SRC_DIR}/fb303_constants.cpp
    ${thrift_SRC_DIR}/FacebookService.cpp
    ${thrift_SRC_DIR}/hive_metastore_types.cpp
    ${thrift_SRC_DIR}/hive_metastore_constants.cpp
    ${thrift_SRC_DIR}/ThriftHiveMetastore.cpp)

file(MAKE_DIRECTORY ${thrift_SRC_DIR})
add_custom_command(
OUTPUT ${thrift_HDRS} ${thrift_SRCS}
COMMAND thrift --gen cpp --recurse
        -out ${thrift_SRC_DIR}/
        -I "${dbcommon_SRC_DIR}/filesystem/hive/"
        "${dbcommon_SRC_DIR}/filesystem/hive/hive_metastore.thrift"
)

AUTO_SOURCES(log_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/log")
AUTO_SOURCES(network_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/network")
AUTO_SOURCES(nodes_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/nodes")
AUTO_SOURCES(utils_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/utils")
AUTO_SOURCES(function_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/function")
AUTO_SOURCES(filesystem_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/filesystem")
AUTO_SOURCES(type_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/type")
AUTO_SOURCES(common_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/common")
AUTO_SOURCES(thread_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/thread")
AUTO_SOURCES(checksum_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/checksum")
AUTO_SOURCES(hash_SRC "*.cc" "RECURSE" "${dbcommon_SRC_DIR}/hash")

LIST(APPEND dbcommon_SOURCES ${log_SRC})
LIST(APPEND dbcommon_SOURCES ${network_SRC})
LIST(APPEND dbcommon_SOURCES ${nodes_SRC})
LIST(APPEND dbcommon_SOURCES ${utils_SRC})
LIST(APPEND dbcommon_SOURCES ${function_SRC})
LIST(APPEND dbcommon_SOURCES ${filesystem_SRC})
LIST(APPEND dbcommon_SOURCES ${type_SRC})
LIST(APPEND dbcommon_SOURCES ${common_SRC})
LIST(APPEND dbcommon_SOURCES ${thread_SRC})
LIST(APPEND dbcommon_SOURCES ${checksum_SRC})
LIST(APPEND dbcommon_SOURCES ${hash_SRC})

LIST(REMOVE_ITEM dbcommon_SOURCES ${CODE_TEMPLATE_SOURCES})

AUTO_SOURCES(log_HEADER "*.h" "${dbcommon_SRC_DIR}/log")
AUTO_SOURCES(network_HEADER "*.h" "${dbcommon_SRC_DIR}/network")
AUTO_SOURCES(nodes_HEADER "*.h" "${dbcommon_SRC_DIR}/nodes")
AUTO_SOURCES(utils_HEADER "*.h" "${dbcommon_SRC_DIR}/utils")
AUTO_SOURCES(utils_mb_HEADER "*.h" "${dbcommon_SRC_DIR}/utils/mb")
AUTO_SOURCES(utils_comp_HEADER "*.h" "${dbcommon_SRC_DIR}/utils/comp")
AUTO_SOURCES(function_HEADER "*.h" "${dbcommon_SRC_DIR}/function")
AUTO_SOURCES(filesystem_HEADER "*.h" "${dbcommon_SRC_DIR}/filesystem")
AUTO_SOURCES(filesystem_hdfs_HEADER "*.h" "${dbcommon_SRC_DIR}/filesystem/hdfs")
AUTO_SOURCES(filesystem_hive_HEADER "*.h" "${dbcommon_SRC_DIR}/filesystem/hive")
AUTO_SOURCES(filesystem_local_HEADER "*.h" "${dbcommon_SRC_DIR}/filesystem/local")
AUTO_SOURCES(type_HEADER "*.h" "${dbcommon_SRC_DIR}/type")
AUTO_SOURCES(common_HEADER "*.h" "${dbcommon_SRC_DIR}/common")
AUTO_SOURCES(common_vector_HEADER "*.h" "${dbcommon_SRC_DIR}/common/vector")
AUTO_SOURCES(thread_HEADER "*.h" "${dbcommon_SRC_DIR}/thread")
AUTO_SOURCES(checksum_HEADER "*.h" "${dbcommon_SRC_DIR}/checksum")
AUTO_SOURCES(testutil_HEADER "*.h" "${dbcommon_SRC_DIR}/testutil")
AUTO_SOURCES(hash_HEADER "*.h" "${dbcommon_SRC_DIR}/hash")

LIST(REMOVE_ITEM function_HEADER ${CODE_TEMPLATE_HEADERS})

SET(CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "")
SET(CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES "")
INCLUDE_DIRECTORIES(${dbcommon_ROOT_DIR})
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/codegen/src)
INCLUDE_DIRECTORIES(${GLOG_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/include)
INCLUDE_DIRECTORIES(/usr/local/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../libhdfs3/build/install/usr/local/hawq/include)
LINK_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/lib)
LINK_DIRECTORIES(/usr/local/lib)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../libhdfs3/build/install/usr/local/hawq/lib)

ADD_LIBRARY(dbcommon-shared SHARED
        ${TEMPLATE_GENERATED_HEADERS}
        ${TEMPLATE_GENERATED_SOURCES}
        ${thrift_SRCS}
        ${dbcommon_SOURCES}
        )
# ADD_LIBRARY(dbcommon-static STATIC ${dbcommon_SOURCES} ${TEMPLATE_GENERATED_HEADERS} ${TEMPLATE_GENERATED_SOURCES})

TARGET_LINK_LIBRARIES(dbcommon-shared ${CLANG_LDFLAGS} ${GLOG_LIBRARIES} iconv pthread hdfs3 z lz4 thrift)
# TARGET_LINK_LIBRARIES(dbcommon-static ${CLANG_LDFLAGS} ${GLOG_LIBRARIES} iconv pthread hdfs3)

SET_TARGET_PROPERTIES(dbcommon-shared PROPERTIES OUTPUT_NAME "dbcommon")
# SET_TARGET_PROPERTIES(dbcommon-static PROPERTIES OUTPUT_NAME "dbcommon")
 
INSTALL(TARGETS dbcommon-shared
     RUNTIME DESTINATION bin
     LIBRARY DESTINATION lib
     ARCHIVE DESTINATION lib)

INSTALL(FILES ${log_HEADER} DESTINATION include/dbcommon/log)
INSTALL(FILES ${network_HEADER} DESTINATION include/dbcommon/network)
INSTALL(FILES ${nodes_HEADER} DESTINATION include/dbcommon/nodes)
INSTALL(FILES ${utils_HEADER} DESTINATION include/dbcommon/utils)
INSTALL(FILES ${common_HEADER} DESTINATION include/dbcommon/common)
INSTALL(FILES ${common_vector_HEADER} DESTINATION include/dbcommon/common/vector)
INSTALL(FILES ${function_HEADER} DESTINATION include/dbcommon/function)
INSTALL(FILES ${TEMPLATE_GENERATED_HEADERS} DESTINATION include/dbcommon/function)
INSTALL(FILES ${filesystem_HEADER} DESTINATION include/dbcommon/filesystem)
INSTALL(FILES ${filesystem_hdfs_HEADER} DESTINATION include/dbcommon/filesystem/hdfs)
INSTALL(FILES ${filesystem_hive_HEADER} DESTINATION include/dbcommon/filesystem/hive)
INSTALL(FILES ${filesystem_local_HEADER} DESTINATION include/dbcommon/filesystem/local)
INSTALL(FILES ${thread_HEADER} DESTINATION include/dbcommon/thread)
INSTALL(FILES ${checksum_HEADER} DESTINATION include/dbcommon/checksum)
INSTALL(FILES ${type_HEADER} DESTINATION include/dbcommon/type)
INSTALL(FILES ${utils_mb_HEADER} DESTINATION include/dbcommon/utils/mb)
INSTALL(FILES ${utils_comp_HEADER} DESTINATION include/dbcommon/utils/comp)
INSTALL(FILES ${testutil_HEADER} DESTINATION include/dbcommon/testutil)
INSTALL(FILES ${hash_HEADER} DESTINATION include/dbcommon/hash)

SET(dbcommon_ROOT_DIR ${dbcommon_ROOT_DIR} PARENT_SCOPE)
