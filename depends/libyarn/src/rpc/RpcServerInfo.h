/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCSERVERINFO_H_
#define _HDFS_LIBHDFS3_RPC_RPCSERVERINFO_H_

#include "Hash.h"

#include <string>
#include <sstream>

namespace Yarn {
namespace Internal {

class RpcServerInfo {
public:

    RpcServerInfo(const std::string & tokenService, const std::string & h, const std::string & p) :
        host(h), port(p), tokenService(tokenService) {
    }

    RpcServerInfo(const std::string & h, uint32_t p) :
        host(h) {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << p;
        port = ss.str();
    }

    size_t hash_value() const;

    bool operator ==(const RpcServerInfo & other) const {
        return this->host == other.host && this->port == other.port && tokenService == other.tokenService;
    }

    const std::string & getTokenService() const {
        return tokenService;
    }

    const std::string & getHost() const {
        return host;
    }

    const std::string & getPort() const {
        return port;
    }

    void setTokenService(const std::string & tokenService) {
        this->tokenService = tokenService;
    }

private:
    std::string host;
    std::string port;
    std::string tokenService;

};

}
}

YARN_HASH_DEFINE(::Yarn::Internal::RpcServerInfo);

#endif /* _HDFS_LIBHDFS3_RPC_RPCSERVERINFO_H_ */
