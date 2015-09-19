/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_RPCCONTENTWRAPPER_H_
#define _HDFS_LIBHDFS3_RPC_RPCCONTENTWRAPPER_H_

#include <google/protobuf/message.h>

#include "WriteBuffer.h"

namespace Yarn {
namespace Internal {

class RpcContentWrapper {
public:
    RpcContentWrapper(::google::protobuf::Message * header,
                      ::google::protobuf::Message * msg);

    int getLength();
    void writeTo(WriteBuffer & buffer);

public:
    ::google::protobuf::Message * header;
    ::google::protobuf::Message * msg;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_RPCCONTENTWRAPPER_H_ */
