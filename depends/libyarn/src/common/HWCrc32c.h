/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_HWCHECKSUM_H_
#define _HDFS_LIBHDFS3_COMMON_HWCHECKSUM_H_

#include "Checksum.h"

namespace Yarn {
namespace Internal {

/**
 * Calculate CRC with hardware support.
 */
class HWCrc32c: public Checksum {
public:
    /**
     * Constructor.
     */
    HWCrc32c() :
        crc(0xFFFFFFFF) {
    }

    uint32_t getValue() {
        return ~crc;
    }

    /**
     * @ref Checksum#reset()
     */
    void reset() {
        crc = 0xFFFFFFFF;
    }

    /**
     * @ref Checksum#update(const void *, int)
     */
    void update(const void * b, int len);

    /**
     * Destory an HWCrc32 instance.
     */
    ~HWCrc32c() {
    }

    /**
     * To test if the hardware support this function.
     * @return true if the hardware support to calculate the CRC.
     */
    static bool available();

private:
    void updateInt64(const char * b, int len);

private:
    uint32_t crc;
};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_HWCHECKSUM_H_ */
