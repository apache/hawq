/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_COMMON_ATOMIC_H_
#define _HDFS_LIBHDFS3_COMMON_ATOMIC_H_

#include "platform.h"

#if defined(NEED_BOOST) && defined(HAVE_BOOST_ATOMIC)

#include <boost/atomic.hpp>

namespace Hdfs {
namespace Internal {

using boost::atomic;

}
}

#elif defined(HAVE_STD_ATOMIC)

#include <atomic>

namespace Hdfs {
namespace Internal {

using std::atomic;

}
}
#else
#error "no atomic library is available"
#endif

#endif /* _HDFS_LIBHDFS3_COMMON_ATOMIC_H_ */

