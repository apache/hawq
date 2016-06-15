# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories (
  ${PROJECT_SOURCE_DIR}/c++/include
  ${PROJECT_SOURCE_DIR}/c++/src
  ${PROJECT_BINARY_DIR}/c++/include
  ${PROJECT_BINARY_DIR}/c++/src
  ${PROTOBUF_INCLUDE_DIRS}
  )

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable (orc-contents
  FileContents.cc
  )

target_link_libraries (orc-contents
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (orc-scan
  FileScan.cc
  )

target_link_libraries (orc-scan
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (orc-metadata
  FileMetadata.cc
  )

target_link_libraries (orc-metadata
  orc
  ${PROTOBUF_LIBRARIES}
  )

 add_executable (orc-statistics
  FileStatistics.cc
  )

target_link_libraries (orc-statistics
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (orc-memory
  FileMemory.cc
  )

target_link_libraries (orc-memory
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (timezone-dump
  TimezoneDump.cc
  )

target_link_libraries (timezone-dump
  orc
  ${PROTOBUF_LIBRARIES}
  )

if (ENABLE_libhdfs3)
  set (LOCAL_Libhdfs3_LIBRARY $ENV{LIBHDFS3_LIBRARY})
  if (LOCAL_Libhdfs3_LIBRARY)
    find_library(Libhdfs3_LIBRARY
                 NAMES hdfs3
                 HINTS
                 LOCAL_Libhdfs3_LIBRARY)
    link_libraries(${Libhdfs3_LIBRARY})
  else ()
    message(FATAL_ERROR "You must specify libhdfs3 link library path inside LIBHDFS3_LIBRARY environment variable!")
  endif (LOCAL_Libhdfs3_LIBRARY)

  add_executable (hdfs-orc-contents
    HdfsFileContents.cc
    )

  target_link_libraries (hdfs-orc-contents
    orc
    ${PROTOBUF_LIBRARIES}
    ${Libhdfs3_LIBRARY}
    )

  add_executable (hdfs-orc-scan
    HdfsFileScan.cc
    )

  target_link_libraries (hdfs-orc-scan
    orc
    ${PROTOBUF_LIBRARIES}
    ${Libhdfs3_LIBRARY}
    )

  add_executable (hdfs-orc-metadata
    HdfsFileMetadata.cc
    )

target_link_libraries (hdfs-orc-metadata
    orc
    ${PROTOBUF_LIBRARIES}
    ${Libhdfs3_LIBRARY}
    )

  add_executable (hdfs-orc-statistics
    HdfsFileStatistics.cc
    )

  target_link_libraries (hdfs-orc-statistics
    orc
    ${PROTOBUF_LIBRARIES}
    ${Libhdfs3_LIBRARY}
    )

  add_executable (hdfs-orc-memory
    HdfsFileMemory.cc
    )

  target_link_libraries (hdfs-orc-memory
    orc
    ${PROTOBUF_LIBRARIES}
    ${Libhdfs3_LIBRARY}
    )
endif (ENABLE_libhdfs3)

if (ENABLE_libhdfs3)
  install(TARGETS
     orc-contents
     orc-metadata
     orc-statistics
     hdfs-orc-contents
     hdfs-orc-scan
     hdfs-orc-metadata
     hdfs-orc-statistics
     hdfs-orc-memory
     DESTINATION bin)
else ()
  install(TARGETS
     orc-contents
     orc-metadata
     orc-statistics
     DESTINATION bin)
endif (ENABLE_libhdfs3)
