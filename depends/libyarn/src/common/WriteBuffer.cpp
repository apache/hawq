/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "WriteBuffer.h"

#include <google/protobuf/io/coded_stream.h>

using namespace google::protobuf::io;
using google::protobuf::uint8;

namespace Yarn {
namespace Internal {

#define WRITEBUFFER_INIT_SIZE 64

WriteBuffer::WriteBuffer() :
    size(0), buffer(WRITEBUFFER_INIT_SIZE) {
}
WriteBuffer::WriteBuffer(const char * buf, size_t s) :
    size(s), buffer() {
    buffer.assign(buf, buf + s);
}

WriteBuffer::~WriteBuffer() {
}

void WriteBuffer::writeVarint32(int32_t value, size_t pos) {
    char buffer[5];
    uint8 * end = CodedOutputStream::WriteVarint32ToArray(value,
                  reinterpret_cast<uint8 *>(buffer));
    write(buffer, reinterpret_cast<char *>(end) - buffer, pos);
}

char * WriteBuffer::alloc(size_t offset, size_t s) {
    assert(offset <= size && size <= buffer.size());

    if (offset > size) {
        return NULL;
    }

    size_t target = offset + s;

    if (target >= buffer.size()) {
        target = target > 2 * buffer.size() ? target : 2 * buffer.size();
        buffer.resize(target);
    }

    size = offset + s;
    return &buffer[offset];
}

void WriteBuffer::write(const void * bytes, size_t s, size_t pos) {
    assert(NULL != bytes);
    assert(pos <= size && pos < buffer.size());
    char * p = alloc(size, s);
    memcpy(p, bytes, s);
}

}
}
