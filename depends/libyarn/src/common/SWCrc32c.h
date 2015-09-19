/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_SWCRC32C_H_
#define _HDFS_LIBHDFS3_COMMON_SWCRC32C_H_

#include "platform.h"

#include "Checksum.h"

#ifdef NEED_BOOST

#include <boost/crc.hpp>

namespace Yarn {
namespace Internal {

typedef boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0xFFFFFFFF, true, true> crc_32c_type;

class SWCrc32c: public Checksum {
public:
    SWCrc32c() {
    }

    uint32_t getValue() {
        return crc.checksum();
    }

    void reset() {
        crc.reset();
    }

    void update(const void * b, int len) {
        crc.process_bytes(b, len);
    }

    ~SWCrc32c() {
    }

private:
    crc_32c_type crc;
};

}
}

#else
namespace Yarn {
namespace Internal {

class SWCrc32c: public Checksum {
public:
    SWCrc32c() :
        crc(0xFFFFFFFF) {
    }

    uint32_t getValue() {
        return ~crc;
    }

    void reset() {
        crc = 0xFFFFFFFF;
    }

    void update(const void * b, int len);

    ~SWCrc32c() {
    }

private:
    uint32_t crc;
};

}
}
#endif

#endif /* _HDFS_LIBHDFS3_COMMON_SWCRC32C_H_ */
