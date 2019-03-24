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

#include <memory>
#include <utility>
#include <vector>

#include "json/json.h"

// #include "kv/common/cn-global.h"
// #include "kv/common/configuration.h"
#include "dbcommon/utils/parameters.h"
#include "dbcommon/utils/url.h"
#include "storage/format/orc/orc-format-writer.h"

namespace storage {
ORCFormatWriter::ORCFormatWriter(
    dbcommon::FileSystemManagerInterface *fsManager, dbcommon::TupleDesc *td,
    const char *fileName, uint32_t blockAlignSize, dbcommon::Parameters *p) {
  this->fsManager = fsManager;
  this->fileName = fileName;

  dbcommon::URL url(fileName);
  this->fileSystem = fsManager->get(url.getNormalizedServiceName());

  std::unique_ptr<orc::Type> schema = buildSchema(td);
  opts.setSchema(std::move(schema));
  opts.setBlockSize(blockAlignSize);

  assert(p != nullptr);
  std::string tableOptionStr = p->get("table.options", "");
  Json::Reader reader;
  Json::Value root;
  if (!reader.parse(tableOptionStr, root))
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "jsoncpp failed to parse \'%s\'",
              tableOptionStr.c_str());
  if (root.isMember("compresstype"))
    opts.setCompressionKind(root["compresstype"].asCString());
  if (root.isMember("rlecoder"))
    opts.setRleVersion(root["rlecoder"].asCString());
  if (root.isMember("dicthreshold"))
    opts.setDictKeySizeThreshold(atof(root["dicthreshold"].asCString()));
  if (root.isMember("bloomfilter")) {
    std::vector<int> columns;
    int col_size = root["bloomfilter"].size();
    for (int i = 0; i < col_size; ++i) {
      columns.push_back(root["bloomfilter"][i].asInt());
    }
    opts.setColumnsToBloomFilter(columns, td->getNumOfColumns());
  }
  if (root.isMember("writestats"))
    opts.setWriteStats(root["writestats"].asBool());

  writer = orc::createWriter(orc::writeFile(fileSystem, url.getPath()), &opts);
}

void ORCFormatWriter::beginWrite() { writer->begin(); }

void ORCFormatWriter::write(dbcommon::TupleBatch *tb) {
  writer->addTupleBatch(tb);
}

void ORCFormatWriter::endWrite() { writer->end(); }

std::unique_ptr<orc::Type> ORCFormatWriter::buildSchema(
    dbcommon::TupleDesc *td) {
  assert(td != nullptr);

  std::vector<dbcommon::TypeKind> &types = td->getColumnTypes();
  std::vector<std::string> &colNames = td->getColumnNames();
  std::vector<int64_t> &colTypeMod = td->getColumnTypeModifiers();

  std::unique_ptr<orc::Type> ret(new orc::TypeImpl(orc::ORCTypeKind::STRUCT));

  for (uint32_t i = 0; i < types.size(); i++) {
    dbcommon::TypeKind t = types[i];
    std::string &name = colNames[i];
    int64_t typeMod = colTypeMod[i];
    std::unique_ptr<orc::Type> child;
    std::unique_ptr<orc::Type> grandchild;

    switch (t) {
      case dbcommon::TypeKind::TINYINTID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::BYTE));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::SMALLINTID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::SHORT));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::INTID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::INT));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::BIGINTID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::LONG));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::FLOATID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::FLOAT));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::DOUBLEID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::DOUBLE));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::STRINGID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::STRING));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::VARCHARID:
        child.reset(
            new orc::TypeImpl(orc::ORCTypeKind::VARCHAR,
                              dbcommon::TypeModifierUtil::getMaxLen(typeMod)));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::CHARID:
        child.reset(
            new orc::TypeImpl(orc::ORCTypeKind::CHAR,
                              dbcommon::TypeModifierUtil::getMaxLen(typeMod)));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::BOOLEANID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::BOOLEAN));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::DATEID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::DATE));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::TIMEID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::TIME));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::BINARYID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::BINARY));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::TIMESTAMPID:
      case dbcommon::TypeKind::TIMESTAMPTZID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::TIMESTAMP));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::DECIMALID:
      case dbcommon::TypeKind::DECIMALNEWID:
        child.reset(
            new orc::TypeImpl(orc::ORCTypeKind::DECIMAL,
                              dbcommon::TypeModifierUtil::getPrecision(typeMod),
                              dbcommon::TypeModifierUtil::getScale(typeMod)));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::SMALLINTARRAYID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::LIST));
        grandchild.reset(new orc::TypeImpl(orc::ORCTypeKind::SHORT));
        child->addStructField(name, std::move(grandchild));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::INTARRAYID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::LIST));
        grandchild.reset(new orc::TypeImpl(orc::ORCTypeKind::INT));
        child->addStructField(name, std::move(grandchild));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::BIGINTARRAYID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::LIST));
        grandchild.reset(new orc::TypeImpl(orc::ORCTypeKind::LONG));
        child->addStructField(name, std::move(grandchild));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::FLOATARRAYID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::LIST));
        grandchild.reset(new orc::TypeImpl(orc::ORCTypeKind::FLOAT));
        child->addStructField(name, std::move(grandchild));
        ret->addStructField(name, std::move(child));
        break;
      case dbcommon::TypeKind::DOUBLEARRAYID:
        child.reset(new orc::TypeImpl(orc::ORCTypeKind::LIST));
        grandchild.reset(new orc::TypeImpl(orc::ORCTypeKind::DOUBLE));
        child->addStructField(name, std::move(grandchild));
        ret->addStructField(name, std::move(child));
        break;
      default:
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "type not supported for orc: %d", t);
    }
  }

  ret->assignIds(0);
  return std::move(ret);
}

}  // namespace storage
