/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_UNORDEREDMAP_H_
#define _HDFS_LIBHDFS3_COMMON_UNORDEREDMAP_H_

#include "platform.h"

#ifdef NEED_BOOST

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

namespace Yarn {
namespace Internal {

using boost::unordered_map;
using boost::unordered_set;

}
}

#else

#include <unordered_map>
#include <unordered_set>

namespace Yarn {
namespace Internal {

using std::unordered_map;
using std::unordered_set;

}
}
#endif

#endif /* _HDFS_LIBHDFS3_COMMON_UNORDEREDMAP_H_ */
