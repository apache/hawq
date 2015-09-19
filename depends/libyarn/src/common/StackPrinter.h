/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_STACK_PRINTER_H_
#define _HDFS_LIBHDFS3_COMMON_STACK_PRINTER_H_

#include "platform.h"

#include <string>

#ifndef DEFAULT_STACK_PREFIX
#define DEFAULT_STACK_PREFIX "\t@\t"
#endif

namespace Yarn {
namespace Internal {

extern const std::string PrintStack(int skip, int maxDepth);

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_STACK_PRINTER_H_ */
