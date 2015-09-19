/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include <google/protobuf/io/coded_stream.h>

#include "RpcContentWrapper.h"

using namespace ::google::protobuf;
using namespace ::google::protobuf::io;

namespace Yarn {
namespace Internal {

RpcContentWrapper::RpcContentWrapper(Message * header, Message * msg) :
    header(header), msg(msg) {
}

int RpcContentWrapper::getLength() {
    int headerLen, msgLen = 0;
    headerLen = header->ByteSize();
    msgLen = msg == NULL ? 0 : msg->ByteSize();
    return headerLen + CodedOutputStream::VarintSize32(headerLen)
           + (msg == NULL ?
              0 : msgLen + CodedOutputStream::VarintSize32(msgLen));
}

void RpcContentWrapper::writeTo(WriteBuffer & buffer) {
    int size = header->ByteSize();
    buffer.writeVarint32(size);
    header->SerializeToArray(buffer.alloc(size), size);

    if (msg != NULL) {
        size = msg->ByteSize();
        buffer.writeVarint32(size);
        msg->SerializeToArray(buffer.alloc(size), size);
    }
}

}
}

