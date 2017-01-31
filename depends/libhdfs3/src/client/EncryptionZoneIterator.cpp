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
#include "EncryptionZoneIterator.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "EncryptionZoneInfo.h"
#include "FileSystemImpl.h"

namespace Hdfs {
EncryptionZoneIterator::EncryptionZoneIterator() :filesystem(NULL), id(0), next(0) {
}

EncryptionZoneIterator::EncryptionZoneIterator(Hdfs::Internal::FileSystemImpl * const fs, 
                                               const int64_t id) :filesystem(fs), id(id), next(0) {
}

EncryptionZoneIterator::EncryptionZoneIterator(const EncryptionZoneIterator & it) :
    filesystem(it.filesystem), id(it.id), next(it.next), lists(it.lists) {
}

EncryptionZoneIterator & EncryptionZoneIterator::operator =(const EncryptionZoneIterator & it) {
    if (this == &it) {
        return *this;
    }

    filesystem = it.filesystem;
    id = it.id;
    next = it.next;
    lists = it.lists;
    return *this;
}

bool EncryptionZoneIterator::listEncryptionZones() {
    bool more;

    if (NULL == filesystem) {
        return false;
    }

    next = 0;
    lists.clear();
    more = filesystem->listEncryptionZones(id, lists);
    if (!lists.empty()){
        id = lists.back().getId();
    }

    return more || !lists.empty();
}

bool EncryptionZoneIterator::hasNext() {
    if (next >= lists.size()) {
        return listEncryptionZones();
    }

    return true;
}

Hdfs::EncryptionZoneInfo EncryptionZoneIterator::getNext() {
    if (next >= lists.size()) {
        if (!listEncryptionZones()) {
            THROW(HdfsIOException, "End of the dir flow");
        }
    }
    return lists[next++];
}

}
