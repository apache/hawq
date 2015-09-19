/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCAUTH_H_
#define _HDFS_LIBHDFS3_RPC_RPCAUTH_H_

#include "libyarncommon/UserInfo.h"
#include "Hash.h"

#include <string>

namespace Yarn {
namespace Internal {

enum AuthMethod {
    SIMPLE = 80, KERBEROS = 81, //"GSSAPI"
    TOKEN = 82, //"DIGEST-MD5"
    UNKNOWN = 255
};

enum AuthProtocol {
    NONE = 0, SASL = -33
};

class RpcAuth {
public:
    RpcAuth() :
        method(SIMPLE) {
    }

    explicit RpcAuth(AuthMethod mech) :
        method(mech) {
    }

    RpcAuth(const UserInfo & ui, AuthMethod mech) :
        method(mech), user(ui) {
    }

    AuthProtocol getProtocol() const {
        return method == SIMPLE ? AuthProtocol::NONE : AuthProtocol::SASL;
    }

    const UserInfo & getUser() const {
        return user;
    }

    UserInfo & getUser() {
        return user;
    }

    void setUser(const UserInfo & user) {
        this->user = user;
    }

    AuthMethod getMethod() const {
        return method;
    }

    void setMethod(AuthMethod method) {
        this->method = method;
    }

    size_t hash_value() const;

    bool operator ==(const RpcAuth & other) const {
        return method == other.method && user == other.user;
    }

public:
    static AuthMethod ParseMethod(const std::string & str);

private:
    AuthMethod method;
    UserInfo user;
};

}
}

YARN_HASH_DEFINE(::Yarn::Internal::RpcAuth);

#endif /* _HDFS_LIBHDFS3_RPC_RPCAUTH_H_ */
