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
#ifndef _HDFS_LIBHDFS3_MOCK_KMSCLIENTPROVIDER_H_
#define _HDFS_LIBHDFS3_MOCK_KMSCLIENTPROVIDER_H_

#include "gmock/gmock.h"

#include "client/KmsClientProvider.h"

using namespace Hdfs::Internal;

class MockKmsClientProvider: public Hdfs::KmsClientProvider {
public:
    MockKmsClientProvider(shared_ptr<RpcAuth> auth, shared_ptr<SessionConfig> conf) : KmsClientProvider(auth, conf) {}
    MOCK_METHOD1(setHttpClient, void(shared_ptr<HttpClient> hc));
    MOCK_METHOD1(getKeyMetadata, ptree(const FileEncryptionInfo &encryptionInfo));
    MOCK_METHOD1(deleteKey, void(const FileEncryptionInfo &encryptionInfo));
    MOCK_METHOD1(decryptEncryptedKey, ptree(const FileEncryptionInfo &encryptionInfo));
    MOCK_METHOD5(createKey, void(const std::string &keyName, const std::string &cipher, const int length, const std::string &material, const std::string &description));

 ptree getEDKResult(FileEncryptionInfo &encryptionInfo) {
    ptree map;
    map.put("name", encryptionInfo.getKeyName());
    map.put("iv", encryptionInfo.getIv());
    map.put("material", KmsClientProvider::base64Encode(encryptionInfo.getKey()));
    return map;
  }

};

#endif /* _HDFS_LIBHDFS3_MOCK_KMSCLIENTPROVIDER_H_ */
