/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "RpcAuth.h"

#include "Exception.h"
#include "ExceptionInternal.h"

namespace Yarn {
namespace Internal {

AuthMethod RpcAuth::ParseMethod(const std::string & str) {
    if (0 == strcasecmp(str.c_str(), "SIMPLE")) {
        return AuthMethod::SIMPLE;
    } else if (0 == strcasecmp(str.c_str(), "KERBEROS")) {
        return AuthMethod::KERBEROS;
    } else if (0 == strcasecmp(str.c_str(), "TOKEN")) {
        return AuthMethod::TOKEN;
    } else {
        THROW(InvalidParameter, "RpcAuth: Unknown auth mechanism type: %s",
              str.c_str());
    }
}

size_t RpcAuth::hash_value() const {
    size_t values[] = { Int32Hasher(method), user.hash_value() };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
