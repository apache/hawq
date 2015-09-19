/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCPROTOCOLINFO_H_
#define _HDFS_LIBHDFS3_RPC_RPCPROTOCOLINFO_H_

#include "Hash.h"

#include <string>

namespace Yarn {
namespace Internal {

class RpcProtocolInfo {
public:
    RpcProtocolInfo(int v, const std::string & p, const std::string & tokenKind) :
        version(v), protocol(p), tokenKind(tokenKind) {
    }

    size_t hash_value() const;

    bool operator ==(const RpcProtocolInfo & other) const {
        return version == other.version && protocol == other.protocol && tokenKind == other.tokenKind;
    }

    const std::string & getProtocol() const {
        return protocol;
    }

    void setProtocol(const std::string & protocol) {
        this->protocol = protocol;
    }

    int getVersion() const {
        return version;
    }

    void setVersion(int version) {
        this->version = version;
    }

    const std::string & getTokenKind() const {
        return tokenKind;
    }

    void setTokenKind(const std::string & tokenKind) {
        this->tokenKind = tokenKind;
    }

private:
    int version;
    std::string protocol;
    std::string tokenKind;

};

}
}

YARN_HASH_DEFINE(::Yarn::Internal::RpcProtocolInfo);

#endif /* _HDFS_LIBHDFS3_RPC_RPCPROTOCOLINFO_H_ */
