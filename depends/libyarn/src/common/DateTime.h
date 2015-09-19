/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_DATETIME_H_
#define _HDFS_LIBHDFS3_COMMON_DATETIME_H_

#include "platform.h"

#include <ctime>
#include <cassert>

#ifdef NEED_BOOST

#include <boost/chrono.hpp>

namespace Yarn {
namespace Internal {

using namespace boost::chrono;

}
}

#else

#include <chrono>

namespace Yarn {
namespace Internal {

using namespace std::chrono;

}
}
#endif

namespace Yarn {
namespace Internal {

template<typename TimeStamp>
static int64_t ToMilliSeconds(TimeStamp const & s, TimeStamp const & e) {
    assert(e >= s);
    return duration_cast<milliseconds>(e - s).count();
}

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_DATETIME_H_ */
