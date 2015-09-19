/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "Hash.h"

#ifdef NEED_BOOST

#include <boost/functional/hash.hpp>

namespace Yarn {
namespace Internal {

/**
 * A hash function object used to hash a boolean value.
 */
boost::hash<bool> BoolHasher;

/**
 * A hash function object used to hash an int value.
 */
boost::hash<int> Int32Hasher;

/**
 * A hash function object used to hash an 64 bit int value.
 */
boost::hash<int64_t> Int64Hasher;

/**
 * A hash function object used to hash a size_t value.
 */
boost::hash<size_t> SizeHasher;

/**
 * A hash function object used to hash a std::string object.
 */
boost::hash<std::string> StringHasher;
}
}

#else

#include <functional>

namespace Yarn {
namespace Internal {

/**
 * A hash function object used to hash a boolean value.
 */
std::hash<bool> BoolHasher;

/**
 * A hash function object used to hash an int value.
 */
std::hash<int> Int32Hasher;

/**
 * A hash function object used to hash an 64 bit int value.
 */
std::hash<int64_t> Int64Hasher;

/**
 * A hash function object used to hash a size_t value.
 */
std::hash<size_t> SizeHasher;

/**
 * A hash function object used to hash a std::string object.
 */
std::hash<std::string> StringHasher;

}
}

#endif
