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
#include "client/FileSystem.h"
#include "client/FileSystemInter.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "gtest/gtest.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"
#include "client/KmsClientProvider.h"
#include "client/HttpClient.h"
#include "client/hdfs.h"

#include <ctime>

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testKmsClient/"

using namespace Hdfs;
using namespace Hdfs::Internal;

class TestKmsClient: public ::testing::Test {
public:
    TestKmsClient() :
            conf("function-test.xml") {
        conf.set("hadoop.kms.authentication.type", "simple");
        conf.set("dfs.encryption.key.provider.uri",
                "kms://http@0.0.0.0:16000/kms");
        sconf.reset(new SessionConfig(conf));
        userInfo.setRealUser("abai");
        auth.reset(new RpcAuth(userInfo, RpcAuth::ParseMethod(sconf->getKmsMethod())));
        hc.reset(new HttpClient());
        kcp.reset(new KmsClientProvider(auth, sconf));
        kcp->setHttpClient(hc);
        fs.reset(new FileSystem(conf));
        fs->connect();
    }

    ~TestKmsClient() {
        try {
            fs->disconnect();
        } catch (...) {
        }
    }
protected:
    Config conf;
    UserInfo userInfo;
    shared_ptr<RpcAuth> auth;
    shared_ptr<HttpClient> hc;
    shared_ptr<KmsClientProvider> kcp;
    shared_ptr<SessionConfig> sconf;
    shared_ptr<FileSystem> fs;
};

TEST_F(TestKmsClient, CreateKeySuccess) {
    std::string keyName = "testcreatekeyname";
    std::string cipher = "AES/CTR/NoPadding";
    int length = 128;
    std::string material = "testCreateKey";
    std::string description = "Test create key success.";
    ASSERT_NO_THROW(
            kcp->createKey(keyName, cipher, length, material, description));
}

TEST_F(TestKmsClient, GetKeyMetadataSuccess) {
    FileEncryptionInfo encryptionInfo;
    encryptionInfo.setKeyName("testcreatekeyname");
    ptree map = kcp->getKeyMetadata(encryptionInfo);
    std::string keyName = map.get < std::string > ("name");
    ASSERT_STREQ("testcreatekeyname", keyName.c_str());
}

TEST_F(TestKmsClient, DeleteKeySuccess) {
    FileEncryptionInfo encryptionInfo;
    encryptionInfo.setKeyName("testcreatekeyname");
    ASSERT_NO_THROW(kcp->deleteKey(encryptionInfo));
}


TEST_F(TestKmsClient, DecryptEncryptedKeySuccess) {
    hdfsFS hfs = NULL;
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    hfs = hdfsBuilderConnect(bld);

    //create key
    hc.reset(new HttpClient());
    kcp.reset(new KmsClientProvider(auth, sconf));
    kcp->setHttpClient(hc);
    std::string keyName = "testdekeyname";
    std::string cipher = "AES/CTR/NoPadding";
    int length = 128;
    std::string material = "test DEK";
    std::string description = "Test DEK create key success.";
    kcp->createKey(keyName, cipher, length, material, description);

    //delete dir
    hdfsDelete(hfs, BASE_DIR"/testDEKey", true);

    //create dir
    EXPECT_EQ(0, hdfsCreateDirectory(hfs, BASE_DIR"/testDEKey"));

    //create encryption zone and encrypted file
    ASSERT_EQ(0,
            hdfsCreateEncryptionZone(hfs, BASE_DIR"/testDEKey", "testdekeyname"));
    std::string hadoop_command = "hadoop fs -touchz ";
    std::string tdeFile = BASE_DIR"/testDEKey/tdefile";
    std::string createFile = hadoop_command + tdeFile;
    std::system(createFile.c_str());

    //decrypt encrypted key
    hc.reset(new HttpClient());
    kcp.reset(new KmsClientProvider(auth, sconf));
    kcp->setHttpClient(hc);
    FileStatus fileStatus = fs->getFileStatus(tdeFile.c_str());
    FileEncryptionInfo *enInfo = fileStatus.getFileEncryption();
    ptree map = kcp->decryptEncryptedKey(*enInfo);
    std::string versionName = map.get < std::string > ("versionName");
    ASSERT_STREQ("EK", versionName.c_str());

    //delete key
    hc.reset(new HttpClient());
    kcp.reset(new KmsClientProvider(auth, sconf));
    kcp->setHttpClient(hc);
    FileEncryptionInfo encryptionInfo;
    encryptionInfo.setKeyName("testdekeyname");
    kcp->deleteKey(encryptionInfo);

}

TEST_F(TestKmsClient, CreateKeyFailediBadUrl) {
    std::string keyName = "testcreatekeyfailname";
    std::string cipher = "AES/CTR/NoPadding";
    std::string material = "testCreateKey";

    std::string url[4] = { "ftp:///http@localhost:16000/kms",
            "kms://htttp@localhost:16000/kms",
            "kms:///httpss@localhost:16000/kms",
            "kms:///http@localhost:16000/kms" };
    for (int i = 0; i < 4; i++) {
        conf.set("hadoop.kms.authentication.type", "simple");
        conf.set("dfs.encryption.key.provider.uri", url[i]);
        sconf.reset(new SessionConfig(conf));
        userInfo.setRealUser("abai");
        auth.reset(new RpcAuth(userInfo, RpcAuth::ParseMethod(sconf->getKmsMethod())));
        hc.reset(new HttpClient());
        kcp.reset(new KmsClientProvider(auth, sconf));
        ASSERT_THROW(kcp->createKey("tesTdeBadUrl", "AES/CTR/NoPadding", 128,
                        "test DEK", "test DEK description"), HdfsIOException);
    }
}


