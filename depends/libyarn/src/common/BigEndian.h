/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_BIGENDIAN_H_
#define _HDFS_LIBHDFS3_COMMON_BIGENDIAN_H_

#include <arpa/inet.h>
#include <cstring>

namespace Yarn {
namespace Internal {

static inline int16_t ReadBigEndian16FromArray(const char * buffer) {
    int16_t retval;
    retval = ntohs(*reinterpret_cast<const int16_t *>(buffer));
    return retval;
}

static inline int32_t ReadBigEndian32FromArray(const char * buffer) {
    int32_t retval;
    retval = ntohl(*reinterpret_cast<const int32_t *>(buffer));
    return retval;
}

static inline char * WriteBigEndian16ToArray(int16_t value, char * buffer) {
    int16_t bigValue = htons(value);
    memcpy(buffer, reinterpret_cast<const char *>(&bigValue), sizeof(int16_t));
    return buffer + sizeof(int16_t);
}

static inline char * WriteBigEndian32ToArray(int32_t value, char * buffer) {
    int32_t bigValue = htonl(value);
    memcpy(buffer, reinterpret_cast<const char *>(&bigValue), sizeof(int32_t));
    return buffer + sizeof(int32_t);
}

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_BIGENDIAN_H_ */
