/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_MEMORY_H_
#define _HDFS_LIBHDFS3_COMMON_MEMORY_H_

#include "platform.h"

#ifdef NEED_BOOST

#include <boost/shared_ptr.hpp>

namespace Yarn {
namespace Internal {

using boost::shared_ptr;

}
}

#else

#include <memory>

namespace Yarn {
namespace Internal {

using std::shared_ptr;

}
}
#endif

#endif /* _HDFS_LIBHDFS3_COMMON_MEMORY_H_ */
