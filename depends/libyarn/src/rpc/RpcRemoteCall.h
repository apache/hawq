/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCREMOTECALL_
#define _HDFS_LIBHDFS3_RPC_RPCREMOTECALL_

#include "DateTime.h"
#include "ExceptionInternal.h"
#include "Memory.h"
#include "RpcCall.h"
#include "RpcProtocolInfo.h"
#include "Thread.h"
#include "WriteBuffer.h"

#define INVALID_RETRY_COUNT -1

namespace Yarn {
namespace Internal {

class RpcRemoteCall;
typedef shared_ptr<RpcRemoteCall> RpcRemoteCallPtr;

class RpcRemoteCall {
public:
    RpcRemoteCall(const RpcCall & c, int32_t id, const std::string & clientId) :
        complete(false), identity(id), call(c), clientId(clientId) {
    }
    virtual ~RpcRemoteCall() {
    }

    virtual void cancel(exception_ptr reason) {
        unique_lock<mutex> lock(mut);
        complete = true;
        error = reason;
        cond.notify_all();
    }

    virtual void serialize(const RpcProtocolInfo & protocol,
                           WriteBuffer & buffer);

    const int32_t getIdentity() const {
        return identity;
    }

    void wait() {
        unique_lock<mutex> lock(mut);

        if (!complete) {
            cond.wait_for(lock, milliseconds(500));
        }
    }

    void check() {
        if (error != exception_ptr()) {
            rethrow_exception(error);
        }
    }

    RpcCall & getCall() {
        return call;
    }

    void done() {
        unique_lock<mutex> lock(mut);
        complete = true;
        cond.notify_all();
    }

    void wakeup() {
        cond.notify_all();
    }

    bool finished() {
        unique_lock<mutex> lock(mut);
        return complete;
    }

public:
    static std::vector<char> GetPingRequest(const std::string & clientid);

private:
    bool complete;
    condition_variable cond;
    const int32_t identity;
    exception_ptr error;
    mutex mut;
    RpcCall call;
    std::string clientId;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_RPCREMOTECALL_ */
