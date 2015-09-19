/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_ATOMIC_H_
#define _HDFS_LIBHDFS3_COMMON_ATOMIC_H_

#include "platform.h"

#ifdef NEED_BOOST

#include <boost/atomic.hpp>

namespace Yarn {
namespace Internal {

using boost::atomic;

}
}

#else

#include <atomic>

namespace Yarn {
namespace Internal {

using std::atomic;

}
}
#endif

#endif /* _HDFS_LIBHDFS3_COMMON_ATOMIC_H_ */

