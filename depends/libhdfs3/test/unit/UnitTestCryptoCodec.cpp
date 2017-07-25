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
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "client/FileSystem.h"
#include "client/FileSystemImpl.h"
#include "client/FileSystemInter.h"
#include "client/OutputStream.h"
#include "client/OutputStreamImpl.h"
#include "client/Packet.h"
#include "client/Pipeline.h"
#include "DateTime.h"
#include "MockFileSystemInter.h"
#include "MockCryptoCodec.h"
#include "MockKmsClientProvider.h"
#include "MockHttpClient.h"
#include "MockLeaseRenewer.h"
#include "MockPipeline.h"
#include "NamenodeStub.h"
#include "server/ExtendedBlock.h"
#include "TestDatanodeStub.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"
#include "client/KmsClientProvider.h"
#include <string>

using namespace Hdfs;
using namespace Hdfs::Internal;
using namespace Hdfs::Mock;
using namespace testing;
using ::testing::AtLeast;


class TestCryptoCodec: public ::testing::Test {
public:
    TestCryptoCodec() {
        
    }

    ~TestCryptoCodec() {
    }

protected:
};

TEST_F(TestCryptoCodec, KmsGetKey_Success) {
    FileEncryptionInfo encryptionInfo;
    encryptionInfo.setKeyName("KmsName");
    encryptionInfo.setIv("KmsIv");
    encryptionInfo.setEzKeyVersionName("KmsVersionName");
    encryptionInfo.setKey("KmsKey");
    Config conf;
    conf.set("hadoop.kms.authentication.type", "simple");
    conf.set("dfs.encryption.key.provider.uri", "kms://http@0.0.0.0:16000/kms");
    shared_ptr<SessionConfig> sconf(new SessionConfig(conf));
    UserInfo userInfo;
    userInfo.setRealUser("abai");
    shared_ptr<RpcAuth> auth(new RpcAuth(userInfo, RpcAuth::ParseMethod(sconf->getKmsMethod())));

    KmsClientProvider kcp(auth, sconf);
    shared_ptr<MockHttpClient> hc(new MockHttpClient());
    kcp.setHttpClient(hc);

    EXPECT_CALL(*hc, post()).Times(1).WillOnce(
            Return(hc->getPostResult(encryptionInfo)));

    ptree map = kcp.decryptEncryptedKey(encryptionInfo);
    std::string KmsKey = map.get < std::string > ("material");

    ASSERT_STREQ("KmsKey", KmsKey.c_str());
}

TEST_F(TestCryptoCodec, encode_Success) {
    FileEncryptionInfo encryptionInfo;
    encryptionInfo.setKeyName("ESKeyName");
    encryptionInfo.setIv("ESIv");
    encryptionInfo.setEzKeyVersionName("ESVersionName");

    Config conf;
    conf.set("hadoop.kms.authentication.type", "simple");
    conf.set("dfs.encryption.key.provider.uri", "kms://http@0.0.0.0:16000/kms");
    shared_ptr<SessionConfig> sconf(new SessionConfig(conf));
    UserInfo userInfo;
    userInfo.setRealUser("abai");
    shared_ptr<RpcAuth> auth(
            new RpcAuth(userInfo, RpcAuth::ParseMethod(sconf->getKmsMethod())));

    shared_ptr<MockKmsClientProvider> kcp(
            new MockKmsClientProvider(auth, sconf));

    //char buf[1024] = "encode hello world";
    char buf[1024];
    Hdfs::FillBuffer(buf, sizeof(buf)-1, 2048);
    buf[sizeof(buf)-1] = 0;

    int32_t bufSize = 1024;

    std::string Key[2] = { "012345678901234567890123456789ab",
            "0123456789012345"};
    for (int i = 0; i < 2; i++) {
        encryptionInfo.setKey(Key[i]);
        shared_ptr<MockHttpClient> hc(new MockHttpClient());
        kcp->setHttpClient(hc);

        EXPECT_CALL(*kcp, decryptEncryptedKey(_)).Times(2).WillRepeatedly(
                Return(kcp->getEDKResult(encryptionInfo)));

        CryptoCodec es(&encryptionInfo, kcp, bufSize);
        es.init(CryptoMethod::ENCRYPT);
        CryptoCodec ds(&encryptionInfo, kcp, bufSize);
        ds.init(CryptoMethod::DECRYPT);


        std::string encodeStr = es.cipher_wrap(buf, strlen(buf));
        ASSERT_NE(0, memcmp(buf, encodeStr.c_str(), strlen(buf)));

        std::string decodeStr = ds.cipher_wrap(encodeStr.c_str(), strlen(buf));
        ASSERT_STREQ(decodeStr.c_str(), buf);
    }
}
