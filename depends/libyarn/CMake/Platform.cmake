# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    SET(OS_LINUX true CACHE INTERNAL "Linux operating system")
ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    SET(OS_MACOSX true CACHE INTERNAL "Mac Darwin operating system")
ELSE(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    MESSAGE(FATAL_ERROR "Unsupported OS: \"${CMAKE_SYSTEM_NAME}\"")
ENDIF(CMAKE_SYSTEM_NAME STREQUAL "Linux")

IF(CMAKE_COMPILER_IS_GNUCXX)
    STRING(REGEX MATCHALL "[0-9]+" GCC_COMPILER_VERSION ${CMAKE_CXX_COMPILER_VERSION})
    
    LIST(GET GCC_COMPILER_VERSION 0 GCC_COMPILER_VERSION_MAJOR)
    LIST(GET GCC_COMPILER_VERSION 1 GCC_COMPILER_VERSION_MINOR)
    
    SET(GCC_COMPILER_VERSION_MAJOR ${GCC_COMPILER_VERSION_MAJOR} CACHE INTERNAL "gcc major version")
    SET(GCC_COMPILER_VERSION_MINOR ${GCC_COMPILER_VERSION_MINOR} CACHE INTERNAL "gcc minor version")
    
    MESSAGE(STATUS "checking compiler: GCC (${GCC_COMPILER_VERSION_MAJOR}.${GCC_COMPILER_VERSION_MINOR}.${GCC_COMPILER_VERSION_PATCH})")
ELSE(CMAKE_COMPILER_IS_GNUCXX)
    EXECUTE_PROCESS(COMMAND ${CMAKE_C_COMPILER} --version  OUTPUT_VARIABLE COMPILER_OUTPUT)
    IF(COMPILER_OUTPUT MATCHES "clang")
        SET(CMAKE_COMPILER_IS_CLANG true CACHE INTERNAL "using clang as compiler")
        MESSAGE(STATUS "checking compiler: CLANG")
    ELSE(COMPILER_OUTPUT MATCHES "clang")
        MESSAGE(FATAL_ERROR "Unsupported compiler: \"${CMAKE_CXX_COMPILER}\"")
    ENDIF(COMPILER_OUTPUT MATCHES "clang")
ENDIF(CMAKE_COMPILER_IS_GNUCXX)




 
 
