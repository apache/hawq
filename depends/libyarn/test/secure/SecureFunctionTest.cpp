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

#include "client/FileSystem.h"
#include "client/FileSystemInter.h"
#include "client/InputStream.h"
#include "client/OutputStream.h"
#include "client/OutputStream.h"
#include "client/Permission.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "gtest/gtest.h"
#include "Logger.h"
#include "Memory.h"
#include "TestUtil.h"
#include "XmlConfig.h"

#include <ctime>

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testSecure/"

using namespace Hdfs;
using namespace Hdfs::Internal;

class TestKerberosConnect : public ::testing::Test {
public:
    TestKerberosConnect() : conf("function-secure.xml") {
        std::stringstream ss;
        ss << "/tmp/krb5cc_";
        ss << getuid();
        const char * userCCpath = GetEnv("LIBHDFS3_TEST_USER_CCPATH",
                                         ss.str().c_str());
        conf.set("hadoop.security.kerberos.ticket.cache.path", userCCpath);
    }

    ~TestKerberosConnect() {
    }

protected:
    Config conf;
};

TEST_F(TestKerberosConnect, connect) {
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
}

TEST_F(TestKerberosConnect, GetDelegationToken_Failure) {
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
    EXPECT_THROW(fs.getDelegationToken(NULL), InvalidParameter);
    EXPECT_THROW(fs.getDelegationToken(""), InvalidParameter);
    fs.disconnect();
}

TEST_F(TestKerberosConnect, DelegationToken_Failure) {
    Token token;
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
    EXPECT_THROW(fs.renewDelegationToken(token), HdfsIOException);
    ASSERT_NO_THROW(token = fs.getDelegationToken("Unknown"));
    EXPECT_THROW(fs.renewDelegationToken(token), AccessControlException);
    ASSERT_NO_THROW(token = fs.getDelegationToken());
    ASSERT_NO_THROW(fs.cancelDelegationToken(token));
    EXPECT_THROW(fs.renewDelegationToken(token), HdfsInvalidBlockToken);
    EXPECT_THROW(fs.cancelDelegationToken(token), HdfsInvalidBlockToken);
    fs.disconnect();
}

TEST_F(TestKerberosConnect, DelegationToken) {
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
    ASSERT_NO_THROW({
        Token token = fs.getDelegationToken();
        fs.renewDelegationToken(token);
        fs.cancelDelegationToken(token);
    });
    fs.disconnect();
}

class TestToken: public ::testing::Test {
public:
    TestToken() {
        Config conf("function-secure.xml");
        std::stringstream ss;
        ss << "/tmp/krb5cc_";
        ss << getuid();
        const char * userCCpath = GetEnv("LIBHDFS3_TEST_USER_CCPATH", ss.str().c_str());
        conf.set("hadoop.security.kerberos.ticket.cache.path", userCCpath);
        fs = shared_ptr<FileSystem> (new FileSystem(conf));
        fs->connect();

        try {
            fs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        fs->mkdirs(BASE_DIR, 0755);
    }

    ~TestToken() {
        try {
            fs->disconnect();
        } catch (...) {
        }
    }

private:
    shared_ptr<FileSystem> fs;
};

static void TestInputOutputStream(FileSystem & fs) {
    OutputStream out;
    ASSERT_NO_THROW(DebugException(out.open(fs, BASE_DIR "file", Create, 0644, true, 0, 1024)));
    std::vector<char> buffer(1024 * 3 + 1);
    FillBuffer(&buffer[0], buffer.size(), 0);
    ASSERT_NO_THROW(DebugException(out.append(&buffer[0], buffer.size())));
    ASSERT_NO_THROW(out.sync());
    InputStream in;
    ASSERT_NO_THROW(DebugException(in.open(fs, BASE_DIR "file")));
    memset(&buffer[0], 0, buffer.size());
    ASSERT_NO_THROW(DebugException(in.readFully(&buffer[0], buffer.size())));
    EXPECT_TRUE(CheckBuffer(&buffer[0], buffer.size(), 0));
    ASSERT_NO_THROW(in.close());
    ASSERT_NO_THROW(out.close());
}

TEST_F(TestToken, BlockToken) {
    TestInputOutputStream(*fs);
}

TEST_F(TestToken, DelegatationToken) {
    Token token;
    ASSERT_NO_THROW(token = fs->getDelegationToken());
    Config conf("function-secure.xml");
    FileSystem tempfs(conf);
    ASSERT_NO_THROW(tempfs.connect(conf.getString("dfs.default.uri"), token));
    TestInputOutputStream(tempfs);
}

