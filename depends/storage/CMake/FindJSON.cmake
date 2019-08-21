# - Find json
# Find the native JSON headers and libraries.
#
#  JSON_INCLUDE_DIRS   - where to find json/json.h, etc.
#  JSON_LIBRARIES      - List of libraries when using json.
#  JSON_FOUND          - True if json found.

#=============================================================================
# Copyright 2006-2009 Kitware, Inc.
# Copyright 2012 Rolf Eike Beer <eike@sf-mail.de>
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

# Look for the header file.
find_path(JSON_INCLUDE_DIR NAMES json/json.h)
mark_as_advanced(JSON_INCLUDE_DIR)

# Look for the library (sorted from most current/relevant entry to least).
find_library(JSON_LIBRARY NAMES jsoncpp
)
mark_as_advanced(JSON_LIBRARY)

# handle the QUIETLY and REQUIRED arguments and set JSON_FOUND to TRUE if
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(JSON DEFAULT_MSG JSON_INCLUDE_DIR JSON_LIBRARY)

if(JSON_FOUND)
  set(JSON_LIBRARIES ${JSON_LIBRARY})
  set(JSON_INCLUDE_DIRS ${JSON_INCLUDE_DIR})
endif()
