/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_FUNCTION_H_
#define _HDFS_LIBHDFS3_COMMON_FUNCTION_H_

#include "platform.h"

#ifdef NEED_BOOST
#include <boost/function.hpp>
#include <boost/bind.hpp>

namespace Yarn {

using boost::function;
using boost::bind;
using boost::reference_wrapper;

}

#else

#include <functional>

namespace Yarn {

using std::function;
using std::bind;
using std::reference_wrapper;
using namespace std::placeholders;

}

#endif

#endif /* _HDFS_LIBHDFS3_COMMON_FUNCTION_H_ */
