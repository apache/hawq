/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "RpcProtocolInfo.h"

namespace Yarn {
namespace Internal {

size_t RpcProtocolInfo::hash_value() const {
    size_t values[] = { Int32Hasher(version), StringHasher(protocol), StringHasher(tokenKind) };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
