/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "RpcConfig.h"

#include <vector>

namespace Yarn {
namespace Internal {

size_t RpcConfig::hash_value() const {
    size_t values[] = { Int32Hasher(maxIdleTime), Int32Hasher(pingTimeout),
                        Int32Hasher(connectTimeout), Int32Hasher(readTimeout), Int32Hasher(
                            writeTimeout), Int32Hasher(maxRetryOnConnect), Int32Hasher(
                            lingerTimeout), Int32Hasher(rpcTimeout), BoolHasher(tcpNoDelay)
                      };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
