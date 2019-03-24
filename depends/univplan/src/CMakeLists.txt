CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(univplan)

FIND_PACKAGE(Protobuf REQUIRED)

SET(univplan_VERSION_MAJOR 0)
SET(univplan_VERSION_MINOR 1)
SET(univplan_VERSION_PATCH 0)
SET(univplan_VERSION_API 1)
set(CMAKE_MACOSX_RPATH 1)

SET(univplan_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
SET(univplan_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/univplan)
SET(univplan_proto_DIR ${univplan_SRC_DIR}/proto)

file(GLOB proto_files "${univplan_SRC_DIR}/proto/*.proto")
set(proto_SRC_DIR ${CMAKE_BINARY_DIR}/src/univplan/proto)
set(UNIVPLAN_PROTO_HDRS
       ${proto_SRC_DIR}/universal-plan.pb.h
       ${proto_SRC_DIR}/universal-plan-catalog.pb.h
       ${proto_SRC_DIR}/universal-plan-expr.pb.h
)
set(UNIVPLAN_PROTO_SRCS
       ${proto_SRC_DIR}/universal-plan.pb.cc
       ${proto_SRC_DIR}/universal-plan-catalog.pb.cc
       ${proto_SRC_DIR}/universal-plan-expr.pb.cc
)
file(MAKE_DIRECTORY ${proto_SRC_DIR})
add_custom_command(
       OUTPUT ${UNIVPLAN_PROTO_SRCS} ${UNIVPLAN_PROTO_HDRS}
       COMMAND ${Protobuf_PROTOC_EXECUTABLE}
       ARGS --cpp_out ${CMAKE_BINARY_DIR}/src -I ${CMAKE_CURRENT_SOURCE_DIR} ${proto_files}
       DEPENDS "${proto_files}"
       )

AUTO_SOURCES(univplan_files "*.cc" "RECURSE" "${univplan_SRC_DIR}")
LIST(APPEND univplan_SOURCES ${univplan_files})

AUTO_SOURCES(univplanbuilder_HEADER "*.h" "${univplan_SRC_DIR}/univplanbuilder")
AUTO_SOURCES(common_HEADER "*.h" "${univplan_SRC_DIR}/common")
AUTO_SOURCES(cwrapper_HEADER "*.h" "${univplan_SRC_DIR}/cwrapper")
AUTO_SOURCES(minmax_HEADER "*.h" "${univplan_SRC_DIR}/minmax")
AUTO_SOURCES(testutil_HEADER "*.h" "${univplan_SRC_DIR}/testutil")

INCLUDE_DIRECTORIES(${univplan_ROOT_DIR})
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/src)
INCLUDE_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/package/include)

LINK_DIRECTORIES(${DEPENDENCY_INSTALL_PREFIX}/package/lib)


ADD_LIBRARY(univplan-shared SHARED
            ${UNIVPLAN_PROTO_HDRS}
            ${UNIVPLAN_PROTO_SRCS}
            ${univplan_SOURCES}
            )
# ADD_LIBRARY(univplan-static STATIC ${univplan_SOURCES} ${UNIVPLAN_PROTO_SRCS} ${UNIVPLAN_PROTO_HDRS})

SET_TARGET_PROPERTIES(univplan-shared PROPERTIES OUTPUT_NAME "univplan")
# SET_TARGET_PROPERTIES(univplan-static PROPERTIES OUTPUT_NAME "univplan")

target_link_libraries(univplan-shared ${CLANG_LDFLAGS} dbcommon glog protobuf pthread)
# target_link_libraries(univplan-static ${CLANG_LDFLAGS} dbcommon glog protobuf pthread)

INSTALL(TARGETS univplan-shared
     RUNTIME DESTINATION bin
     LIBRARY DESTINATION lib
     ARCHIVE DESTINATION lib)

INSTALL(FILES ${univplanbuilder_HEADER} DESTINATION include/univplan/univplanbuilder)
INSTALL(FILES ${common_HEADER} DESTINATION include/univplan/common)
INSTALL(FILES ${cwrapper_HEADER} DESTINATION include/univplan/cwrapper)
INSTALL(FILES ${minmax_HEADER} DESTINATION include/univplan/minmax)
INSTALL(FILES ${testutil_HEADER} DESTINATION include/univplan/testutil)
INSTALL(FILES ${UNIVPLAN_PROTO_HDRS} DESTINATION include/univplan/proto)

SET(univplan_ROOT_DIR ${univplan_ROOT_DIR} PARENT_SCOPE)
