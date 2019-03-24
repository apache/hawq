/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/utils/parameters.h"
#include "dbcommon/utils/url.h"

#include "storage/format/format.h"
#include "storage/format/orc/orc-format.h"

#include "univplan/univplanbuilder/univplanbuilder-scan-task.h"

namespace storage {

std::unique_ptr<Format> Format::createFormat(
    univplan::UNIVPLANFORMATTYPE type) {
  return createFormat(type, nullptr);
}

std::unique_ptr<Format> Format::createFormat(univplan::UNIVPLANFORMATTYPE type,
                                             dbcommon::Parameters *p) {
  std::unique_ptr<Format> format;
  switch (type) {
    case univplan::UNIVPLANFORMATTYPE::ORC_FORMAT: {
      format.reset(new ORCFormat(p));
      break;
    }
    default: {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "invalid format %d", type);
    }
  }
  return std::move(format);
}

//
// we use fileLenghts as input since if we implement transaction,
// we can not get file EOF correctly without knowing more information.
//
// for now, we simply allocate average length of file total size(avgLength) to
// each task.
// the splits in each task may contains several files(their total length equals
// to avgLength).
//
std::unique_ptr<univplan::UnivPlanScanFileSplitListList> Format::createTasks(
    const std::vector<std::unique_ptr<Input> > &inputs, int nWorker) {
  LOG_INFO("createTasks is called");

  assert(nWorker > 0);

  std::unique_ptr<univplan::UnivPlanScanFileSplitListList> taskList(
      new univplan::UnivPlanScanFileSplitListList);
  // create one scan task to contain all splits
  univplan::UnivPlanBuilderScanTask scanTaskBld;
  for (const std::unique_ptr<Input> &file : inputs) {
    FileInput *fi = static_cast<FileInput *>(file.get());
    dbcommon::URL urlParser(fi->getName());
    dbcommon::FileSystem *fs =
        fsManager->get(urlParser.getNormalizedServiceName());
    std::vector<std::unique_ptr<dbcommon::FileBlockLocation> > locations =
        fs->getFileBlockLocation(urlParser.getPath().c_str(), 0, fi->getSize());
    for (const std::unique_ptr<dbcommon::FileBlockLocation> &loc : locations) {
      scanTaskBld.addScanFileSplit(fi->getName().c_str(), loc->offset,
                                   loc->length, -1, -1);  // no rangeid, rgid
    }
  }
  // build scan task by transfering tb from this builder to fmt instance
  std::unique_ptr<univplan::UnivPlanScanFileSplitListTb> newScanTask(
      new univplan::UnivPlanScanFileSplitListTb(
          std::move(scanTaskBld.releaseSplitsTb())));

  // newScanTask->debugOuput();

  taskList->push_back(std::move(newScanTask));
  return std::move(taskList);
}

}  // namespace storage
