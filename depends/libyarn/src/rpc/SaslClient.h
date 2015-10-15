/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_
#define _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_

#include <gsasl.h>

#include "libyarncommon/Token.h"
#include "network/Socket.h"
#include "RpcAuth.h"
#include "YARNRpcHeader.pb.h"

namespace Yarn {
namespace Internal {

#define SWITCH_TO_SIMPLE_AUTH -88

class SaslClient {
public:
    SaslClient(const hadoop::common::RpcSaslProto_SaslAuth & auth, const Token & token,
               const std::string & principal);

    ~SaslClient();

    std::string evaluateChallenge(const std::string & chanllege);

    bool isComplete();

private:
    void initKerberos(const hadoop::common::RpcSaslProto_SaslAuth & auth,
                      const std::string & principal);
    void initDigestMd5(const hadoop::common::RpcSaslProto_SaslAuth & auth, const Token & token);

private:
    Gsasl * ctx;
    Gsasl_session * session;
    bool complete;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_ */
