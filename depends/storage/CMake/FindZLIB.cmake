# - Find zlib
# Find the native ZLIB headers and libraries.
#
#  ZLIB_INCLUDE_DIRS   - where to find zlib.h, etc.
#  ZLIB_LIBRARIES      - List of libraries when using zlib.
#  ZLIB_FOUND          - True if zlib found.

#=============================================================================
# (C) 1995-2017 Jean-loup Gailly and Mark Adler
#
#  This software is provided 'as-is', without any express or implied
#  warranty.  In no event will the authors be held liable for any damages
#  arising from the use of this software.
#
#  Permission is granted to anyone to use this software for any purpose,
#  including commercial applications, and to alter it and redistribute it
#  freely, subject to the following restrictions:
#
#  1. The origin of this software must not be misrepresented; you must not
#     claim that you wrote the original software. If you use this software
#     in a product, an acknowledgment in the product documentation would be
#     appreciated but is not required.
#  2. Altered source versions must be plainly marked as such, and must not be
#     misrepresented as being the original software.
#  3. This notice may not be removed or altered from any source distribution.
#
#  Jean-loup Gailly        Mark Adler
#  jloup@gzip.org          madler@alumni.caltech.edu
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

# Look for the header file.
find_path(ZLIB_INCLUDE_DIR NAMES zlib.h)
mark_as_advanced(ZLIB_INCLUDE_DIR)

# Look for the library (sorted from most current/relevant entry to least).
find_library(ZLIB_LIBRARY NAMES z)
mark_as_advanced(ZLIB_LIBRARY)

# handle the QUIETLY and REQUIRED arguments and set ZLIB_FOUND to TRUE if
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ZLIB DEFAULT_MSG ZLIB_INCLUDE_DIR ZLIB_LIBRARY)

if(ZLIB_FOUND)
  set(ZLIB_LIBRARIES ${ZLIB_LIBRARY})
  set(ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIR})
endif()
