/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_COMMON_SWCRC32C_H_
#define _HDFS_LIBHDFS3_COMMON_SWCRC32C_H_

#include "platform.h"

#include "Checksum.h"

#ifdef NEED_BOOST

#include <boost/crc.hpp>

namespace Hdfs {
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
namespace Hdfs {
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
