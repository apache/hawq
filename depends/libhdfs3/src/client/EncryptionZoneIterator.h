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
#ifndef _HDFS_LIBHFDS3_CLIENT_ENCRYPTIONZONE_ITERATOR_H_
#define _HDFS_LIBHFDS3_CLIENT_ENCRYPTIONZONE_ITERATOR_H_

#include "FileStatus.h"
#include "EncryptionZoneInfo.h"
#include <vector>

namespace Hdfs {
namespace Internal {
class FileSystemImpl;
}

class EncryptionZoneIterator {
public:
    EncryptionZoneIterator();
    EncryptionZoneIterator(Hdfs::Internal::FileSystemImpl * const fs,
                          const int64_t id);
    EncryptionZoneIterator(const EncryptionZoneIterator & it);
    EncryptionZoneIterator & operator = (const EncryptionZoneIterator & it);
    bool hasNext();
    EncryptionZoneInfo getNext();

private:
    bool listEncryptionZones();

private:
    Hdfs::Internal::FileSystemImpl * filesystem;
    int64_t id;
    size_t next;
    std::vector<EncryptionZoneInfo> lists;
};

}

#endif /* _HDFS_LIBHFDS3_CLIENT_ENCRYPTIONZONE_ITERATOR_H_ */
