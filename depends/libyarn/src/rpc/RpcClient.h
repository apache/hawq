/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCCLIENT_H_
#define _HDFS_LIBHDFS3_RPC_RPCCLIENT_H_

#include "Memory.h"
#include "RpcAuth.h"
#include "RpcCall.h"
#include "RpcChannel.h"
#include "RpcChannelKey.h"
#include "RpcConfig.h"
#include "RpcProtocolInfo.h"
#include "RpcServerInfo.h"
#include "Thread.h"
#include "Unordered.h"

#include <vector>

#ifdef MOCK
#include "TestRpcChannelStub.h"
#endif

namespace Yarn {
namespace Internal {

class RpcClient {
public:
    /**
     * Destroy an RpcClient instance.
     */
    virtual ~RpcClient() {
    }

    /**
     * Get a RPC channel, create a new one if necessary.
     * @param auth Authentication information used to setup RPC connection.
     * @param protocol The RPC protocol used in this call.
     * @param server Remote server information.
     * @param conf RPC connection configuration.
     * @param once If true, the RPC channel will not be reused.
     */
    virtual RpcChannel & getChannel(const RpcAuth & auth,
                                    const RpcProtocolInfo & protocol, const RpcServerInfo & server,
                                    const RpcConfig & conf) = 0;

    /**
     * Check the RpcClient is still running.
     * @return true if the RpcClient is still running.
     */
    virtual bool isRunning() = 0;

    virtual std::string getClientId() const = 0;

    virtual int32_t getCallId() = 0;

public:
    static RpcClient & getClient();
    static void createSinglten();

private:
    static once_flag once;
    static shared_ptr<RpcClient> client;
};

class RpcClientImpl: public RpcClient {
public:
    /**
     * Construct a RpcClient.
     */
    RpcClientImpl();

    /**
     * Destroy an RpcClient instance.
     */
    ~RpcClientImpl();

    /**
     * Get a RPC channel, create a new one if necessary.
     * @param auth Authentication information used to setup RPC connection.
     * @param protocol The RPC protocol used in this call.
     * @param server Remote server information.
     * @param conf RPC connection configuration.
     * @param once If true, the RPC channel will not be reused.
     */
    RpcChannel & getChannel(const RpcAuth & auth,
                            const RpcProtocolInfo & protocol, const RpcServerInfo & server,
                            const RpcConfig & conf);

    /**
     * Close the RPC channel.
     */
    void close();

    /**
     * Check the RpcClient is still running.
     * @return true if the RpcClient is still running.
     */
    bool isRunning();

    std::string getClientId() const {
        return clientId;
    }

    int32_t getCallId() {
        static mutex mutid;
        lock_guard<mutex> lock(mutid);
        ++count;
        count = count < std::numeric_limits<int32_t>::max() ? count : 0;
        return count;
    }

private:
    shared_ptr<RpcChannel> createChannelInternal(
        const RpcChannelKey & key);

    void clean();

private:
    atomic<bool> cleaning;
    atomic<bool> running;
    condition_variable cond;
    int64_t count;
    mutex mut;
    std::string clientId;
    thread cleaner;
    unordered_map<RpcChannelKey, shared_ptr<RpcChannel> > allChannels;

#ifdef MOCK
private:
    /*
     * for test
     */
    Hdfs::Mock::TestRpcChannelStub * stub;
#endif
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_RPCCLIENT_H_ */
