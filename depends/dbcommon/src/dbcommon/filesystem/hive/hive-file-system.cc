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

#include "dbcommon/filesystem/hive/hive-file-system.h"
#include "dbcommon/log/logger.h"

#include "dbcommon/thrift/ThriftHiveMetastore.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include "thrift/transport/TSocket.h"

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;

namespace dbcommon {
void getHiveDataDirectory(const std::string &host, int port,
                          std::string const &dbname, std::string const &tblname,
                          std::string *hiveTablePath, StatusHive *status) {
  try {
    boost::shared_ptr<TSocket> socket(new TSocket(host, port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

    hive::ThriftHiveMetastoreClient client(protocol);
    transport->open();
    hive::Table table;
    client.get_table(table, dbname, tblname);
    *hiveTablePath = (table.sd.location);
    transport->close();
  } catch (std::exception &e) {
    LOG_WARNING("failed to get info of table %s.%s, Error: %s", dbname.c_str(),
                tblname.c_str(), e.what());
    status->errorMessage = "failed to get table info:";
    status->errorCode = ERRCODE_CONNECTION_FAILURE;
    return;
  }
  status->errorCode = ERRCODE_SUCCESSFUL_COMPLETION;
}
}  // namespace dbcommon
