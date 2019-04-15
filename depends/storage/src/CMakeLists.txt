CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(storage)

FIND_PACKAGE(Protobuf REQUIRED)
FIND_PACKAGE(JSON REQUIRED)
FIND_PACKAGE(Snappy REQUIRED)
FIND_PACKAGE(ZLIB REQUIRED)

SET(storage_VERSION_MAJOR 0)
SET(storage_VERSION_MINOR 1)
SET(storage_VERSION_PATCH 0)
SET(storage_VERSION_API 1)
set(CMAKE_MACOSX_RPATH 1)

SET(storage_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
SET(storage_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/storage)
SET(orcformat_proto_DIR ${storage_SRC_DIR}/format/orc)
SET(Protobuf_PROTOC_EXECUTABLE /usr/local/bin/protoc)

file(GLOB proto_files "${storage_SRC_DIR}/format/orc/*.proto")
set(proto_SRC_DIR ${CMAKE_BINARY_DIR}/src/storage/format/orc)
set(storage_PROTO_HDRS
       ${proto_SRC_DIR}/orc_proto.pb.h
)
set(storage_PROTO_SRCS
       ${proto_SRC_DIR}/orc_proto.pb.cc
)
file(MAKE_DIRECTORY ${proto_SRC_DIR})
add_custom_command(
       OUTPUT ${storage_PROTO_SRCS} ${storage_PROTO_HDRS}
       COMMAND ${Protobuf_PROTOC_EXECUTABLE}
       ARGS --cpp_out ${CMAKE_BINARY_DIR}/src -I ${CMAKE_CURRENT_SOURCE_DIR} ${proto_files}
       DEPENDS "${proto_files}"
       )

AUTO_SOURCES(storage_files "*.cc" "RECURSE" "${storage_SRC_DIR}")
LIST(APPEND storage_SOURCES ${storage_files})

AUTO_SOURCES(common_HEADER "*.h" "${storage_SRC_DIR}/common")
AUTO_SOURCES(cwrapper_HEADER "*.h" "${storage_SRC_DIR}/cwrapper")
AUTO_SOURCES(format_HEADER "*.h" "${storage_SRC_DIR}/format")
AUTO_SOURCES(orc_format_HEADER "*.h" "${storage_SRC_DIR}/format/orc")
AUTO_SOURCES(testutil_HEADER "*.h" "${storage_SRC_DIR}/testutil")

INCLUDE_DIRECTORIES(${storage_ROOT_DIR})
#INCLUDE_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/package/include)
INCLUDE_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/include)
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/src)
INCLUDE_DIRECTORIES(/usr/local/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../univplan/build/install/usr/local/hawq/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../dbcommon/build/install/usr/local/hawq/include)
LINK_DIRECTORIES(/usr/local/lib)
#LINK_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/package/lib)
LINK_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/lib)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../libhdfs3/build/install/usr/local/hawq/lib)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../univplan/build/install/usr/local/hawq/lib)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../../dbcommon/build/install/usr/local/hawq/lib)


ADD_LIBRARY(storage-shared SHARED ${storage_SOURCES} ${storage_PROTO_SRCS} ${storage_PROTO_HDRS})
# ADD_LIBRARY(storage-static STATIC ${storage_SOURCES} ${storage_PROTO_SRCS} ${storage_PROTO_HDRS})

SET_TARGET_PROPERTIES(storage-shared PROPERTIES OUTPUT_NAME "storage")
# SET_TARGET_PROPERTIES(storage-static PROPERTIES OUTPUT_NAME "storage")

target_link_libraries(storage-shared ${CLANG_LDFLAGS}
                      dbcommon
                      univplan
                      hdfs3
                      snappy
                      lz4
                      ${ZLIB_LIBRARIES}
                      ${JSON_LIBRARIES}
                      glog
                      protobuf
                      pthread
                      iconv)
# target_link_libraries(storage-static ${CLANG_LDFLAGS} dbcommon univplan hdfs3 snappy lz4 ${ZLIB_LIBRARIES} ${JSON_LIBRARIES} glog protobuf pthread iconv)

INSTALL(TARGETS storage-shared
     RUNTIME DESTINATION bin
     LIBRARY DESTINATION lib
     ARCHIVE DESTINATION lib)

INSTALL(FILES ${common_HEADER} DESTINATION include/storage/common)
INSTALL(FILES ${cwrapper_HEADER} DESTINATION include/storage/cwrapper)
INSTALL(FILES ${format_HEADER} DESTINATION include/storage/format)
INSTALL(FILES ${orc_format_HEADER} DESTINATION include/storage/format/orc)
INSTALL(FILES ${storage_PROTO_HDRS} DESTINATION include/storage/format/orc)
INSTALL(FILES ${testutil_HEADER} DESTINATION include/storage/testutil)

SET(storage_ROOT_DIR ${storage_ROOT_DIR} PARENT_SCOPE)
