/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "Hash.h"
#include "Token.h"

using namespace Yarn::Internal;

namespace Yarn {

size_t Token::hash_value() const {
    size_t values[] = { StringHasher(identifier), StringHasher(password),
                        StringHasher(kind), StringHasher(service)
                      };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
