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
#include "DirectoryIterator.h"
#include "FileStatus.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemImpl.h"

namespace Hdfs {

DirectoryIterator::DirectoryIterator() :
    needLocations(false), hasMore(true), filesystem(NULL), next(0) {
}

DirectoryIterator::DirectoryIterator(Hdfs::Internal::FileSystemImpl * const fs,
                                     std::string path, bool needLocations) :
    needLocations(needLocations), hasMore(true), filesystem(fs), next(0), path(path) {
}

DirectoryIterator::DirectoryIterator(const DirectoryIterator & it) :
    needLocations(it.needLocations), hasMore(it.hasMore), filesystem(it.filesystem),
    next(it.next), path(it.path), startAfter(it.startAfter), lists(it.lists) {
}

DirectoryIterator & DirectoryIterator::operator =(const DirectoryIterator & it) {
    if (this == &it) {
        return *this;
    }

    needLocations = it.needLocations;
    hasMore = it.hasMore;
    filesystem = it.filesystem;
    next = it.next;
    path = it.path;
    startAfter = it.startAfter;
    lists = it.lists;
    return *this;
}

bool DirectoryIterator::getListing() {
    if (NULL == filesystem || !hasMore) {
        return false;
    }

    next = 0;
    lists.clear();

    hasMore = filesystem->getListing(path, startAfter, needLocations, lists);

    if (hasMore && !lists.empty()) {
        startAfter = lists.back().getPath();
    }

    return !lists.empty();
}

bool DirectoryIterator::hasNext() {
    if (next < lists.size()) {
        return true;
    }

    return getListing();
}

Hdfs::FileStatus DirectoryIterator::getNext() {
    if (!hasNext()) {
        THROW(HdfsIOException, "End of the dir flow");
    }

    return lists[next++];
}

}
