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
#ifndef _HDFS_LIBHDFS3_MOCK_CRYPTOCODEC_H_
#define _HDFS_LIBHDFS3_MOCK_CRYPTOCODEC_H_

#include "gmock/gmock.h"

#include "client/CryptoCodec.h"
#include "client/KmsClientProvider.h"

class MockCryptoCodec: public Hdfs::CryptoCodec {
public:
  MockCryptoCodec(FileEncryptionInfo *encryptionInfo, shared_ptr<KmsClientProvider> kcp, int32_t bufSize) : CryptoCodec(encryptionInfo, kcp, bufSize) {}

  MOCK_METHOD2(init, int(CryptoMethod crypto_method, int64_t stream_offset));
  MOCK_METHOD2(cipher_wrap, std::string(const char * buffer,int64_t size));
};

#endif /* _HDFS_LIBHDFS3_MOCK_CRYPTOCODEC_H_ */
