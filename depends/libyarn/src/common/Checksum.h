/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_CHECKSUM_H_
#define _HDFS_LIBHDFS3_COMMON_CHECKSUM_H_

#include <stdint.h>

#define CHECKSUM_TYPE_SIZE 1
#define CHECKSUM_BYTES_PER_CHECKSUM_SIZE 4
#define CHECKSUM_TYPE_CRC32C 2

namespace Yarn {
namespace Internal {

/**
 * An abstract base CRC class.
 */
class Checksum {
public:
    /**
     * @return Returns the current checksum value.
     */
    virtual uint32_t getValue() = 0;

    /**
     * Resets the checksum to its initial value.
     */
    virtual void reset() = 0;

    /**
     * Updates the current checksum with the specified array of bytes.
     * @param b The buffer of data.
     * @param len The buffer length.
     */
    virtual void update(const void * b, int len) = 0;

    /**
     * Destroy the instance.
     */
    virtual ~Checksum() {
    }
};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_CHECKSUM_H_ */
