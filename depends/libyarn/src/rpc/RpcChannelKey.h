/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCCHANNELKEY_H_
#define _HDFS_LIBHDFS3_RPC_RPCCHANNELKEY_H_

#include "libyarncommon/Token.h"
#include "Hash.h"
#include "RpcAuth.h"
#include "RpcConfig.h"
#include "RpcProtocolInfo.h"
#include "RpcServerInfo.h"
#include <Memory.h>

namespace Yarn {
namespace Internal {

class RpcChannelKey {
public:
    RpcChannelKey(const RpcAuth & a, const RpcProtocolInfo & p,
                  const RpcServerInfo & s, const RpcConfig & c);

public:
    size_t hash_value() const;

    const RpcAuth & getAuth() const {
        return auth;
    }

    const RpcConfig & getConf() const {
        return conf;
    }

    const RpcProtocolInfo & getProtocol() const {
        return protocol;
    }

    const RpcServerInfo & getServer() const {
        return server;
    }

    bool operator ==(const RpcChannelKey & other) const {
        return this->auth == other.auth && this->protocol == other.protocol
               && this->server == other.server && this->conf == other.conf
               && ((token == NULL && other.token == NULL)
                   || (token && other.token && *token == *other.token));
    }

    const Token & getToken() const {
        assert(token != NULL);
        return *token;
    }

    bool hasToken() {
        return token != NULL;
    }

private:
    const RpcAuth auth;
    const RpcConfig conf;
    const RpcProtocolInfo protocol;
    const RpcServerInfo server;
    shared_ptr<Token> token;
};

}
}

YARN_HASH_DEFINE(::Yarn::Internal::RpcChannelKey);

#endif /* _HDFS_LIBHDFS3_RPC_RPCCHANNELKEY_H_ */
