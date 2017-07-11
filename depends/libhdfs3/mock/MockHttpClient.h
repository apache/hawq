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
#ifndef _HDFS_LIBHDFS3_MOCK_HTTPCLIENT_H_
#define _HDFS_LIBHDFS3_MOCK_HTTPCLIENT_H_

#include "gmock/gmock.h"

#include "client/HttpClient.h"
#include "client/KmsClientProvider.h"
#include <boost/property_tree/ptree.hpp>

using boost::property_tree::ptree;

class MockHttpClient: public Hdfs::HttpClient {
public:
  MOCK_METHOD0(post, std::string());
  MOCK_METHOD0(del, std::string());
  MOCK_METHOD0(put, std::string());
  MOCK_METHOD0(get, std::string());

  std::string getPostResult(FileEncryptionInfo &encryptionInfo) {
	ptree map;
	map.put("name", encryptionInfo.getKeyName());
	map.put("iv", encryptionInfo.getIv());
	map.put("material", encryptionInfo.getKey());

	std::string json = KmsClientProvider::toJson(map);
	return json;
  }


};

#endif /* _HDFS_LIBHDFS3_MOCK_HTTPCLIENT_H_ */
