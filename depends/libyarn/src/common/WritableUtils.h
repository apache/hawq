/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS_3_UTIL_WritableUtils_H_
#define _HDFS_LIBHDFS_3_UTIL_WritableUtils_H_

#include <string>

namespace Yarn {
namespace Internal {

class WritableUtils {
public:
    WritableUtils(char * b, size_t l);

    bool ReadInt32(int32_t * value);

    bool ReadInt64(int64_t * value);

    bool ReadRaw(char * buf, size_t size);

    bool ReadString(std::string & str);

    bool ReadText(std::string & str);

    bool readByte(char * byte);

    size_t WriteInt32(int32_t value);

    size_t WriteInt64(int64_t value);

    bool WriteRaw(const char * buf, size_t size);

private:
    int decodeWritableUtilsSize(int value);

    int readByte();

    void writeByte(int val);

    bool isNegativeWritableUtils(int value);

    int32_t ReadBigEndian32();

private:
    char * buffer;
    size_t len;
    size_t current;
};

}
}
#endif /* _HDFS_LIBHDFS_3_UTIL_WritableUtils_H_ */
