/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "RpcServerInfo.h"

#include <string>

namespace Yarn {
namespace Internal {

size_t RpcServerInfo::hash_value() const {
    size_t values[] = { StringHasher(host), StringHasher(port), StringHasher(tokenService) };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
