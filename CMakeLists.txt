CMAKE_MINIMUM_REQUIRED(VERSION 3.12)
PROJECT(hawq)

SET(generate_source
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/parser/gram.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/catalog/caql/gram.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/catalog/caql/catquery.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/catalog/core/catcoretable.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/bootstrap/bootparse.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/port/dynloader.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/port/pg_sema.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/port/pg_shmem.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/utils/fmgrtab.c
        )
set_source_files_properties(
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/parser/gram.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/catalog/caql/gram.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/catalog/caql/catquery.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/catalog/core/catcoretable.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/bootstrap/bootparse.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/port/dynloader.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/port/pg_sema.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/port/pg_shmem.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/backend/utils/fmgrtab.c
        PROPERTIES GENERATED TRUE
)
add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/config.log
        COMMAND ./configure --enable-orca --with-python
        COMMAND make skip-orca-build
        COMMAND make -C src/port/ pg_config_paths.h
        COMMAND make -C src/backend/
        ../../src/include/utils/hawq_funcoid_mapping.h
        ../../src/include/utils/fmgroids.h
        ../../src/include/utils/probes.h
        ../../src/include/parser/gram.h
        COMMAND make -C src/backend/catalog/caql/ catquery.c gram.c scan.c
        COMMAND make -C src/backend/catalog/core/ catcoretable.c
        COMMAND make -C src/backend/bootstrap/ bootparse.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
add_custom_target(config
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/config.log
        )


FILE(GLOB cdb_parquet_source "${CMAKE_CURRENT_SOURCE_DIR}/src/backend/access/parquet/metadatautil_c++/*.cpp")

FILE(GLOB_RECURSE dxltranslators_source "${CMAKE_CURRENT_SOURCE_DIR}/src/backend/gpopt/*.cpp")

FILE(GLOB_RECURSE pg_timezone_source "${CMAKE_CURRENT_SOURCE_DIR}/src/timezone/*.c")
LIST(FILTER pg_timezone_source EXCLUDE REGEX ".*/timezone/zic.c")

FILE(GLOB_RECURSE pg_port_source "${CMAKE_CURRENT_SOURCE_DIR}/src/port/*.c")
LIST(FILTER pg_port_source INCLUDE REGEX "chklocale|copydir|dirmod|exec|noblock|path|pgsleep|pgstrcasecmp|qsort|qsort_arg|sprompt|thread|strlcpy|strlcat|pg_crc32c")

FILE(GLOB_RECURSE pg_regex_source "${CMAKE_CURRENT_SOURCE_DIR}/src/backend/regex/*.c")
LIST(FILTER pg_regex_source INCLUDE REGEX "regcomp|regerror|regexec|regfree")


FILE(GLOB_RECURSE cdb_source "${CMAKE_CURRENT_SOURCE_DIR}/src/backend/*.c")

LIST(FILTER cdb_source EXCLUDE REGEX ".*/test/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/test_discard/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/mb/conversion_procs/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/mb/win1251.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/mb/win866.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/mb/iso.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/index/catquery.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/bootscanner.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/scan.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/guc-file.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/cdbdistributedxidmap.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/nodeParquetScan.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/nodeSeqscan.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/like_match.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/win32.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/regex/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/beos/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/dynloader/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/nextstep/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/qnx4/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/win32/.*")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/ipc_test.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/posix_sema.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/win32_sema.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/win32_shmem.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/sysv_sema.c")
LIST(FILTER cdb_source EXCLUDE REGEX ".*/port/sysv_shmem.c")
LIST(APPEND cdb_source ${pg_port_source} ${pg_timezone_source} ${cdb_parquet_source} ${pg_regex_source})

if (CMAKE_SYSTEM_NAME STREQUAL Darwin)
    ADD_DEFINITIONS(-DGPOS_Darwin)
endif (CMAKE_SYSTEM_NAME STREQUAL Darwin)
if (CMAKE_SYSTEM_NAME STREQUAL Linux)
    ADD_DEFINITIONS(-D_GNU_SOURCE)
    LIST(FILTER cdb_source EXCLUDE REGEX "src/backend/port/darwin/system.c")
endif (CMAKE_SYSTEM_NAME STREQUAL Linux)
ADD_DEFINITIONS(-DDLSUFFIX="so")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2")

include_directories(src/include)
include_directories(src/backend/resourcemanager/include)
include_directories(src/backend/gp_libpq_fe)
include_directories(src/port)

include_directories(${CMAKE_SOURCE_DIR}/hornet/dbcommon/src)
include_directories(${CMAKE_SOURCE_DIR}/hornet/univplan/src)
include_directories(${CMAKE_SOURCE_DIR}/hornet/magma/src)
include_directories(${CMAKE_SOURCE_DIR}/hornet/storage/src)
include_directories(${CMAKE_SOURCE_DIR}/hornet/executor/src)
include_directories(${CMAKE_BINARY_DIR}/hornet/dbcommon/src)
include_directories(/opt/dependency/package/include)

link_directories(/opt/dependency-Darwin/package/lib)
link_directories(/opt/dependency/package/lib)

# gporca
add_library(dxltranslators ${dxltranslators_source})
add_dependencies(dxltranslators config)

# postgres
add_executable(postgres ${cdb_source} ${generate_source})
target_link_libraries(postgres z bz2 lz4 snappy xml2 curl ldap json-c krb5 yarn thrift) # basic
target_link_libraries(postgres gpos xerces-c naucrates gpdbcost gpopt dxltranslators) # gp-orca
target_link_libraries(postgres hdfs3 dbcommon-shared univplan-shared storage-shared magma-client-shared storage-magma-format-shared executor-shared) # hornet
target_link_libraries(postgres dl)
add_dependencies(postgres config)

# pluggable storage
add_library(orc.so contrib/orc/orc.c)
add_library(magma.so contrib/magma/magma.c)

# libpq
FILE(GLOB_RECURSE libpq_source "${CMAKE_CURRENT_SOURCE_DIR}/src/interfaces/libpq/*.c")
LIST(FILTER libpq_source EXCLUDE REGEX "win32.c")
LIST(APPEND libpq_source src/backend/libpq/ip.c)
LIST(APPEND libpq_source src/backend/libpq/md5.c)
LIST(APPEND libpq_source src/backend/utils/mb/encnames.c)
LIST(APPEND libpq_source src/backend/utils/mb/wchar.c)
add_library(pq ${libpq_source})
add_library(pgport ${pg_port_source})
target_link_libraries(pq ldap pgport)
target_compile_options(pq PRIVATE -DFRONTEND)
add_dependencies(pq config)
add_dependencies(pgport config)

# feature-test
set(CMAKE_CXX_STANDARD 11)
FILE(GLOB_RECURSE feature_test_source "${CMAKE_CURRENT_SOURCE_DIR}/src/test/feature/*.cpp")
LIST(FILTER feature_test_source EXCLUDE REGEX "/Ranger/")
LIST(FILTER feature_test_source EXCLUDE REGEX "/userPoc/")
LIST(FILTER feature_test_source EXCLUDE REGEX "/sqlreport/")
add_executable(feature-test ${feature_test_source})
target_include_directories(feature-test PUBLIC src/test/feature)
target_include_directories(feature-test PUBLIC src/test/feature/lib)
target_include_directories(feature-test PUBLIC src/interfaces/libpq)
target_link_libraries(feature-test xml2 gtest pq)
target_link_libraries(feature-test pthread)
set_target_properties(feature-test
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/src/test/feature/"
        )
