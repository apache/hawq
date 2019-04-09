CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

AUTO_SOURCES(unit_SOURCES "*.cc" "RECURSE" ${CMAKE_CURRENT_SOURCE_DIR})
AUTO_SOURCES(testutil_SRC "*.cc" "RECURSE" "${dbcommon_ROOT_DIR}/dbcommon/testutil")
LIST(APPEND unit_SOURCES ${testutil_SRC})

FIND_PACKAGE(GTest REQUIRED)

INCLUDE_DIRECTORIES(${dbcommon_ROOT_DIR}/test/unit)
INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIRS})

# cogapp generation
find_package(Cogapp)

SET(CODE_TEMPLATE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/function/test-codegen-arith-functions.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/function/test-codegen-cmp-functions.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/function/test-codegen-typecast-functions.cc
)
COGAPP_GENERATE(TEMPLATE_GENERATED_SOURCES ${CODE_TEMPLATE_SOURCES})
LIST(REMOVE_ITEM unit_SOURCES ${CODE_TEMPLATE_SOURCES})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-narrowing")

ADD_EXECUTABLE(unit EXCLUDE_FROM_ALL
    ${TEMPLATE_GENERATED_SOURCES}
    ${unit_SOURCES}
)

target_link_libraries(unit ${CLANG_LDFLAGS} hdfs3 dbcommon-shared gtest gmock)
