/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "RpcChannelKey.h"

#include <vector>

namespace Yarn {
namespace Internal {

RpcChannelKey::RpcChannelKey(const RpcAuth & a, const RpcProtocolInfo & p,
                             const RpcServerInfo & s, const RpcConfig & c) :
    auth(a), conf(c), protocol(p), server(s) {
    const Token * temp = auth.getUser().selectToken(protocol.getTokenKind(),
                         server.getTokenService());

    if (temp) {
        token = shared_ptr<Token> (new Token(*temp));
    }
}

size_t RpcChannelKey::hash_value() const {
    size_t tokenHash = token ? token->hash_value() : 0;
    size_t values[] = { auth.hash_value(), protocol.hash_value(),
                        server.hash_value(), conf.hash_value(), tokenHash
                      };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
