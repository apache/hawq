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
#include "client/hdfs.h"
#include "client/HttpClient.h"
#include "client/KmsClientProvider.h"
#include "client/FileEncryptionInfo.h"
#include "Logger.h"
#include "SessionConfig.h"
#include "TestUtil.h"
#include "XmlConfig.h"

#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <limits>
#include <stdlib.h>
#include <sstream>
#include <iostream>
#include <openssl/md5.h>
#include <stdio.h>

using namespace Hdfs::Internal;

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testCInterface/"
#define MAXDATABUFF 1024
#define MD5LENTH 33

using namespace std;
using Hdfs::CheckBuffer;

static bool ReadFully(hdfsFS fs, hdfsFile file, char * buffer, size_t length) {
    int todo = length, rc;

    while (todo > 0) {
        rc = hdfsRead(fs, file, buffer + (length - todo), todo);

        if (rc <= 0) {
            return false;
        }

        todo = todo - rc;
    }

    return true;
}

static bool CreateFile(hdfsFS fs, const char * path, int64_t blockSize,
                       int64_t fileSize) {
    hdfsFile out;
    size_t offset = 0;
    int64_t todo = fileSize, batch;
    std::vector<char> buffer(32 * 1024);
    int rc = -1;

    do {
        if (NULL == (out = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, blockSize))) {
            break;
        }

        while (todo > 0) {
            batch = todo < static_cast<int32_t>(buffer.size()) ?
                    todo : buffer.size();
            Hdfs::FillBuffer(&buffer[0], batch, offset);

            if (0 > (rc = hdfsWrite(fs, out, &buffer[0], batch))) {
                break;
            }

            todo -= rc;
            offset += rc;
        }

        rc = hdfsCloseFile(fs, out);
    } while (0);

    return rc >= 0;
}

static void fileMD5(const char* strFilePath, char* result) {
    MD5_CTX ctx;
    int len = 0;
    unsigned char buffer[1024] = { 0 };
    unsigned char digest[16] = { 0 };
    FILE *pFile = fopen(strFilePath, "rb");
    MD5_Init(&ctx);
    while ((len = fread(buffer, 1, 1024, pFile)) > 0) {
        MD5_Update(&ctx, buffer, len);
    }
    MD5_Final(digest, &ctx);
    fclose(pFile);
    int i = 0;
    char tmp[3] = { 0 };
    for (i = 0; i < 16; i++) {
        sprintf(tmp, "%02X", digest[i]);
        strcat(result, tmp);
    }
}

static void bufferMD5(const char* strFilePath, int size, char* result) {
    unsigned char digest[16] = { 0 };
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, strFilePath, size);
    MD5_Final(digest, &ctx);
    int i = 0;
    char tmp[3] = { 0 };
    for (i = 0; i < 16; i++) {
        sprintf(tmp, "%02X", digest[i]);
        strcat(result, tmp);
    }
}

static void diff_file2buffer(const char *file_path, const char *buf) {
    std::cout << "diff file: " << file_path << std::endl;
    char resultFile[MD5LENTH] = { 0 };
    char resultBuffer[MD5LENTH] = { 0 };

    fileMD5(file_path, resultFile);
    std::cout << "resultFile is " << resultFile << std::endl;

    bufferMD5(buf, strlen(buf), resultBuffer);
    std::cout << "resultBuf is " << resultBuffer << std::endl;

    ASSERT_STREQ(resultFile, resultBuffer);
}

bool CheckFileContent(hdfsFS fs, const char * path, int64_t len, size_t offset) {
    hdfsFile in = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);

    if (in == NULL) {
        return false;
    }

    std::vector<char> buff(1 * 1024 * 1024);
    int rc, todo = len, batch;

    while (todo > 0) {
        batch = todo < static_cast<int>(buff.size()) ? todo : buff.size();
        batch = hdfsRead(fs, in, &buff[0], batch);

        if (batch <= 0) {
            hdfsCloseFile(fs, in);
            return false;
        }

        todo = todo - batch;
        rc = Hdfs::CheckBuffer(&buff[0], batch, offset);
        offset += batch;

        if (!rc) {
            hdfsCloseFile(fs, in);
            return false;
        }
    }

    hdfsCloseFile(fs, in);
    return true;
}

int64_t GetFileLength(hdfsFS fs, const char * path) {
    int retval;
    hdfsFileInfo * info = hdfsGetPathInfo(fs, path);

    if (!info) {
        return -1;
    }

    retval = info->mSize;
    hdfsFreeFileInfo(info, 1);
    return retval;
}

TEST(TestCInterfaceConnect, TestConnect_InvalidInput) {
    hdfsFS fs = NULL;
    //test invalid input
    fs = hdfsConnect(NULL, 50070);
    EXPECT_TRUE(fs == NULL && EINVAL == errno);
    fs = hdfsConnect("hadoop.apache.org", 80);
    EXPECT_TRUE(fs == NULL && EIO == errno);
    fs = hdfsConnect("localhost", 22);
    EXPECT_TRUE(fs == NULL && EIO == errno);
}

static void ParseHdfsUri(const std::string & uri, std::string & host, int & port) {
    std::string str = uri;
    char * p = &str[0], *q;

    if (0 == strncasecmp(p, "hdfs://", strlen("hdfs://"))) {
        p += strlen("hdfs://");
    }

    q = strchr(p, ':');

    if (NULL == q) {
        port = 0;
    } else {
        *q++ = 0;
        port = strtol(q, NULL, 0);
    }

    host = p;
}

TEST(TestCInterfaceConnect, TestConnect_Success) {
    hdfsFS fs = NULL;
    char * uri = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    //test valid input
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsBuilderSetUserName(bld, "test");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
    std::string host;
    int port;
    ASSERT_EQ(0, hdfsConfGetStr("dfs.default.uri", &uri));
    ParseHdfsUri(uri, host, port);
    hdfsConfStrFree(uri);
    fs = hdfsConnectAsUser(host.c_str(), port, USER);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    fs = hdfsConnectAsUserNewInstance(host.c_str(), port, USER);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    fs = hdfsConnectNewInstance(host.c_str(), port);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
}

TEST(TestCInterfaceTDE, DISABLED_TestCreateEnRPC_Success) {
    hdfsFS fs = NULL;
    hdfsEncryptionZoneInfo * enInfo = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    //Test TDE API.
    system("hadoop fs -rmr /TDEEnRPC");
    system("hadoop key create keytderpc");
    system("hadoop fs -mkdir /TDEEnRPC");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEEnRPC", "keytderpc"));
    enInfo = hdfsGetEZForPath(fs, "/TDEEnRPC");
    ASSERT_TRUE(enInfo != NULL);
    ASSERT_STREQ("keytderpc", enInfo->mKeyName);
    std::cout << "----hdfsEncryptionZoneInfo----:" << " KeyName : " << enInfo->mKeyName << " Suite : " << enInfo->mSuite << " CryptoProtocolVersion : " << enInfo->mCryptoProtocolVersion << " Id : " << enInfo->mId << " Path : " << enInfo->mPath << std::endl;
    hdfsFreeEncryptionZoneInfo(enInfo, 1);
    //Test create multiple encryption zones.
    for (int i = 0; i < 10; i++){
        std::stringstream newstr;
        newstr << i;
        std::string tde = "/TDEEnRPC" + newstr.str();
        std::string key = "keytderpc" + newstr.str();
        std::string rmTde = "hadoop fs -rmr /TDEEnRPC" + newstr.str();
        std::string tdeKey = "hadoop key create keytderpc" + newstr.str();
        std::string mkTde = "hadoop fs -mkdir /TDEEnRPC" + newstr.str();
        system(rmTde.c_str());
        system(tdeKey.c_str());
        system(mkTde.c_str());
        ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, tde.c_str(), key.c_str()));
    }
    int num = 0;
    hdfsListEncryptionZones(fs, &num);
    EXPECT_EQ(num, 12);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}

TEST(TestCInterfaceTDE, TestOpenCreateWithTDE_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
    ASSERT_TRUE(fs != NULL);
    //Create encryption zone for test.
    system("hadoop fs -rmr /TDEOpen");
    system("hadoop key create keytde4open");
    system("hadoop fs -mkdir /TDEOpen");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEOpen", "keytde4open"));
    //Create tdefile under the encryption zone for TDE to write.
    const char *tdefile = "/TDEOpen/testfile";;
    //Write buffer to tdefile.
    const char *buffer = "test tde open file with create flag success";
    hdfsFile out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_CREAT, 0, 0, 0);
    ASSERT_TRUE(out != NULL)<< hdfsGetLastError();
    EXPECT_EQ(strlen(buffer), hdfsWrite(fs, out, (const void *)buffer, strlen(buffer)))
            << hdfsGetLastError();
    hdfsCloseFile(fs, out);
    //Read buffer from tdefile with hadoop API.
    FILE *file = popen("hadoop fs -cat /TDEOpen/testfile", "r");
    char bufGets[128];
    while (fgets(bufGets, sizeof(bufGets), file)) {
    }
    pclose(file);
    //Check the buffer is eaqual to the data reading from tdefile.
    ASSERT_STREQ(bufGets, buffer);
    system("hadoop fs -rmr /TDEOpen");
    system("hadoop key delete keytde4open -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}


TEST(TestCInterfaceTDE, TestAppendOnceWithTDE_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
    ASSERT_TRUE(fs != NULL);
    //Create encryption zone for test.
    system("hadoop fs -rmr /TDEAppend1");
    //system("hadoop key delete keytde4append1 -f");
    system("hadoop key create keytde4append1");
    system("hadoop fs -mkdir /TDEAppend1");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEAppend1", "keytde4append1"));
    //Create tdefile under the encryption zone for TDE to write.
    const char *tdefile = "/TDEAppend1/testfile";
    ASSERT_TRUE(CreateFile(fs, tdefile, 0, 0));
    //Write buffer to tdefile.
    const char *buffer = "test tde append once success";
    hdfsFile out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(out != NULL)<< hdfsGetLastError();
    EXPECT_EQ(strlen(buffer), hdfsWrite(fs, out, (const void *)buffer, strlen(buffer)))
            << hdfsGetLastError();
    hdfsCloseFile(fs, out);
    //Read buffer from tdefile with hadoop API.
    FILE *file = popen("hadoop fs -cat /TDEAppend1/testfile", "r");
    char bufGets[128];
    while (fgets(bufGets, sizeof(bufGets), file)) {
    }
    pclose(file);
    //Check the buffer is eaqual to the data reading from tdefile.
    ASSERT_STREQ(bufGets, buffer);
    system("hadoop fs -rmr /TDEAppend1");
    system("hadoop key delete keytde4append1 -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}

TEST(TestCInterfaceTDE, TestMultipleAppendReopenfileWithTDE_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
    ASSERT_TRUE(fs != NULL);
    //Create encryption zone for test.
    system("hadoop fs -rmr /TDEAppend2");
    system("hadoop key delete keytde4append2 -f");
    system("hadoop key create keytde4append2");
    system("hadoop fs -mkdir /TDEAppend2");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEAppend2", "keytde4append2"));
    //Create tdefile under the encryption zone for TDE to write.
    const char *tdefile = "/TDEAppend2/testfile";
    ASSERT_TRUE(CreateFile(fs, tdefile, 0, 0));
    //Write buffer to tdefile.
    std::string buffer1 = "test tde multiple append";
    std::string buffer2 = "with reopen file success";
    std::string buffer = buffer1 + buffer2;
    hdfsFile out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(out != NULL)<< hdfsGetLastError();
    EXPECT_EQ(buffer1.length(), hdfsWrite(fs, out, (const void *)buffer1.c_str(), buffer1.length()))
            << hdfsGetLastError();
    hdfsCloseFile(fs, out);
    //Reopen tdefile to append buffer.
    out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_APPEND, 0, 0, 0);
    EXPECT_EQ(buffer2.length(), hdfsWrite(fs, out, (const void *)buffer2.c_str(), buffer2.length())) << hdfsGetLastError();
    hdfsCloseFile(fs, out);
    //Read buffer from tdefile with hadoop API.
    FILE *file = popen("hadoop fs -cat /TDEAppend2/testfile", "r");
    char bufGets[128];
    while (fgets(bufGets, sizeof(bufGets), file)) {
    }
    pclose(file);
    //Check the buffer is eaqual to the data reading from tdefile.
    ASSERT_STREQ(bufGets, buffer.c_str());
    system("hadoop fs -rmr /TDEAppend2");
    system("hadoop key delete keytde4append2 -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}


TEST(TestCInterfaceTDE, TestMultipleAppendfileWithTDE_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
    ASSERT_TRUE(fs != NULL);
    //Create encryption zone for test.
    system("hadoop fs -rmr /TDEAppend3");
    system("hadoop key delete keytde4append3 -f");
    system("hadoop key create keytde4append3");
    system("hadoop fs -mkdir /TDEAppend3");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEAppend3", "keytde4append3"));
    //Create tdefile under the encryption zone for TDE to write.
    const char *tdefile = "/TDEAppend3/testfile";
    ASSERT_TRUE(CreateFile(fs, tdefile, 0, 0));
    //Write buffer to tdefile with multiple append.
    int size = 3 * 128;
    size_t offset = 0;
    hdfsFile out;
    int64_t todo = size;
    std::vector<char> buffer(size);
    int rc = -1;
    do {
        if (NULL == (out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_APPEND, 0, 0, 1024))) {
            break;
        }
        Hdfs::FillBuffer(&buffer[0], 128 * 3, 1024);
        while (todo > 0) {
            if (0 > (rc = hdfsWrite(fs, out, &buffer[offset], 128))) {
                break;
            }
            todo -= rc;
            offset += rc;
        }
        rc = hdfsCloseFile(fs, out);
    } while (0);

    //Read buffer from tdefile with hadoop API.
    FILE *file = popen("hadoop fs -cat /TDEAppend3/testfile", "r");
    char bufGets[128];
    while (fgets(bufGets, sizeof(bufGets), file)) {
    }
    pclose(file);
    //Check the buffer's md5 value is eaqual to the tdefile's md5 value.
    system("rm -rf ./testfile");
    system("hadoop fs -get /TDEAppend3/testfile ./");
    char resultFile[MD5LENTH] = { 0 };
    fileMD5("./testfile", resultFile);
    char resultBuffer[MD5LENTH] = { 0 };
    bufferMD5(&buffer[0], size, resultBuffer);
    ASSERT_STREQ(resultFile, resultBuffer);
    system("rm ./testfile");
    system("hadoop fs -rmr /TDEAppend3");
    system("hadoop key delete keytde4append3 -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}


TEST(TestCInterfaceTDE, TestAppendWithTDEMultipleChunks_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    //creake key and encryption zone
    system("hadoop fs -rmr /TDEAppend4");
    system("hadoop key delete keytde4append4 -f");
    system("hadoop key create keytde4append4");
    system("hadoop fs -mkdir /TDEAppend4");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEAppend4", "keytde4append4"));
    const char *tdefile = "/TDEAppend4/testfile";
    ASSERT_TRUE(CreateFile(fs, tdefile, 0, 0));
    //Write buffer to tdefile.
    int size = 1024;
    size_t offset = 0;
    hdfsFile out;
    int64_t todo = size;
	int64_t batch;
    std::vector<char> buffer(size);
    int rc = -1;
    do {
        if (NULL == (out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_APPEND, 0, 0, 1024))) {
            break;
        }
        while (todo > 0) {
            batch = todo < static_cast<int32_t>(buffer.size()) ?
                    todo : buffer.size();

            Hdfs::FillBuffer(&buffer[0], batch, offset);

            if (0 > (rc = hdfsWrite(fs, out, &buffer[offset], batch))) {
                break;
            }
            LOG(INFO, "todo is %d. offset is %d", todo, offset);
            todo -= rc;
            offset += rc;
        }
        rc = hdfsCloseFile(fs, out);
    } while (0);
    //Check the testfile's md5 value is equal to buffer's md5 value.
    system("rm -rf ./testfile");
    system("hadoop fs -get /TDEAppend4/testfile ./");
    char resultFile[MD5LENTH] = { 0 };
    fileMD5("./testfile", resultFile);
    char resultBuffer[MD5LENTH] = { 0 };
    bufferMD5(&buffer[0], size, resultBuffer);
    ASSERT_STREQ(resultFile, resultBuffer);
    system("rm ./testfile");
    system("hadoop fs -rmr /TDEAppend4");
    system("hadoop key delete keytde4append4 -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}

TEST(TestCInterfaceTDE, TestAppendWithTDEMultipleBlocks_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    //creake key and encryption zone
    system("hadoop fs -rmr /TDEAppend5");
    system("hadoop key delete keytde4append5 -f");
    system("hadoop key create keytde4append5");
    system("hadoop fs -mkdir /TDEAppend5");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDEAppend5", "keytde4append5"));
    const char *tdefile = "/TDEAppend5/testfile";
    ASSERT_TRUE(CreateFile(fs, tdefile, 0, 0));
    //Write buffer to tdefile.
    int size = 256 * 1024 * 1024;
    size_t offset = 0;
    hdfsFile out;
    int64_t todo = size;
    int64_t batch;
    std::vector<char> buffer(size);
    int rc = -1;
    do {
        if (NULL == (out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_APPEND, 0, 0, 1024))) {
            break;
        }
        while (todo > 0) {
            batch = todo < static_cast<int32_t>(buffer.size()) ?
                    todo : buffer.size();

            Hdfs::FillBuffer(&buffer[0], batch, offset);

            if (0 > (rc = hdfsWrite(fs, out, &buffer[offset], batch))) {
                break;
            }
            LOG(INFO, "todo is %d. offset is %d", todo, offset);
            todo -= rc;
            offset += rc;
        }
        rc = hdfsCloseFile(fs, out);
    } while (0);
    //Check the testfile's md5 value is equal to buffer's md5 value.
    system("rm -rf ./testfile");
    system("hadoop fs -get /TDEAppend5/testfile ./");
    char resultFile[MD5LENTH] = { 0 };
    fileMD5("./testfile", resultFile);
    char resultBuffer[MD5LENTH] = { 0 };
    bufferMD5(&buffer[0], size, resultBuffer);
    ASSERT_STREQ(resultFile, resultBuffer);
    system("rm ./testfile");
    system("hadoop fs -rmr /TDEAppend5");
    system("hadoop key delete keytde4append5 -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}

TEST(TestCInterfaceTDE, TestAppendMultiTimes_Success) {
    hdfsFS fs = NULL;
    hdfsEncryptionZoneInfo * enInfo = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);

    //creake iey and encryption zone
    system("hadoop fs -rmr /TDE");
    system("hadoop key delete keytde4append -f");
    system("hadoop key create keytde4append");
    system("hadoop fs -mkdir /TDE");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDE", "keytde4append"));
    enInfo = hdfsGetEZForPath(fs, "/TDE");
    ASSERT_TRUE(enInfo != NULL);
    EXPECT_TRUE(enInfo->mKeyName != NULL);
    hdfsFreeEncryptionZoneInfo(enInfo, 1);

    hdfsFile out;
    //case2: close and append
    const char *tdefile2 = "/TDE/testfile2";
    char out_data2[] = "12345678";
    ASSERT_TRUE(CreateFile(fs, tdefile2, 0, 0));
    out = hdfsOpenFile(fs, tdefile2, O_WRONLY | O_APPEND, 0, 0, 0);
    hdfsWrite(fs, out, out_data2, 4);
    hdfsCloseFile(fs, out);

    out = hdfsOpenFile(fs, tdefile2, O_WRONLY | O_APPEND, 0, 0, 0);
    hdfsWrite(fs, out, out_data2+4, 4);
    hdfsCloseFile(fs, out);
    system("rm ./testfile2");
    system("hadoop fs -get /TDE/testfile2 ./");
    diff_file2buffer("testfile2", out_data2);

    //case3: multi-append
    const char *tdefile3 = "/TDE/testfile3";
    char out_data3[] = "1234567812345678123456781234567812345678123456781234567812345678"; //16*4byte
    ASSERT_TRUE(CreateFile(fs, tdefile3, 0, 0));
    out = hdfsOpenFile(fs, tdefile3, O_WRONLY | O_APPEND, 0, 0, 0);
    hdfsWrite(fs, out, out_data3, 5);
    hdfsWrite(fs, out, out_data3+5, 28);
    hdfsWrite(fs, out, out_data3+33, 15);
    hdfsWrite(fs, out, out_data3+48, 16);
    hdfsCloseFile(fs, out);
    system("rm ./testfile3");
    system("hadoop fs -get /TDE/testfile3 ./");

    diff_file2buffer("testfile3", out_data3);


    //case4: multi-append > bufsize(8k)
    const char *tdefile4 = "/TDE/testfile4";
    int data_size = 13*1024+1;
    char *out_data4 = (char *)malloc(data_size);
    Hdfs::FillBuffer(out_data4, data_size-1, 1024);
    out_data4[data_size-1] = 0;
    ASSERT_TRUE(CreateFile(fs, tdefile4, 0, 0));
    out = hdfsOpenFile(fs, tdefile4, O_WRONLY | O_APPEND, 0, 0, 0);

    int todo = 0;
    int offset = 0;
    todo = 9*1024-1;
    while (todo > 0) {
        int rc = 0;
        if (0 > (rc = hdfsWrite(fs, out, out_data4+offset, todo))) {
            break;
        }
        todo -= rc;
        offset += rc;
    }

    todo = 4*1024+1;
    while (todo > 0) {
        int rc = 0;
        if (0 > (rc = hdfsWrite(fs, out, out_data4+offset, todo))) {
            break;
        }
        todo -= rc;
        offset += rc;
    }

    ASSERT_EQ(data_size-1, offset);

    hdfsCloseFile(fs, out);
    system("rm ./testfile4");
    system("hadoop fs -get /TDE/testfile4 ./");
    diff_file2buffer("testfile4", out_data4);
    free(out_data4);



    system("hadoop fs -rmr /TDE");
    system("hadoop key delete keytde4append -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}

TEST(TestErrorMessage, TestErrorMessage) {
    EXPECT_NO_THROW(hdfsGetLastError());
    hdfsChown(NULL, TEST_HDFS_PREFIX, NULL, NULL);
    LOG(LOG_ERROR, "%s", GetSystemErrorInfo(EINVAL));
    EXPECT_STREQ(GetSystemErrorInfo(EINVAL), hdfsGetLastError());
}

class TestCInterface: public ::testing::Test {
public:
    TestCInterface() {
        setenv("LIBHDFS3_CONF", "function-test.xml", 1);
        struct hdfsBuilder * bld = hdfsNewBuilder();
        assert(bld != NULL);
        hdfsBuilderSetNameNode(bld, "default");
        hdfsBuilderSetForceNewInstance(bld);
        fs = hdfsBuilderConnect(bld);

        if (fs == NULL) {
            throw std::runtime_error("cannot connect hdfs");
        }

        hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
        superfs = hdfsBuilderConnect(bld);
        hdfsFreeBuilder(bld);

        if (superfs == NULL) {
            throw std::runtime_error("cannot connect hdfs");
        }

        std::vector<char> buffer(128);
        hdfsSetWorkingDirectory(superfs, hdfsGetWorkingDirectory(fs, &buffer[0], buffer.size()));
        hdfsDelete(superfs, BASE_DIR, true);

        if (0 != hdfsCreateDirectory(superfs, BASE_DIR)) {
            throw std::runtime_error("cannot create test directory");
        }

        if (0 != hdfsChown(superfs, TEST_HDFS_PREFIX, USER, NULL)) {
            throw std::runtime_error("cannot set owner for test directory");
        }

        if (0 != hdfsChown(superfs, BASE_DIR, USER, NULL)) {
            throw std::runtime_error("cannot set owner for test directory");
        }
    }

    ~TestCInterface() {
        hdfsDelete(superfs, BASE_DIR, true);
        hdfsDisconnect(fs);
        hdfsDisconnect(superfs);
    }

protected:
    hdfsFS fs;
    hdfsFS superfs;
};

TEST_F(TestCInterface, TestGetConf) {
    char * output = NULL;
    EXPECT_EQ(-1, hdfsConfGetStr(NULL, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetStr("test.get.conf", NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetStr(NULL, &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetStr("not exist", &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(0, hdfsConfGetStr("test.get.conf", &output));
    EXPECT_STREQ("success", output);
    hdfsConfStrFree(output);
}

TEST_F(TestCInterface, TestGetConfInt32) {
    int output;
    EXPECT_EQ(-1, hdfsConfGetInt(NULL, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetInt("test.get.confint32", NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetInt(NULL, &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetInt("not exist", &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(0, hdfsConfGetInt("test.get.confint32", &output));
    EXPECT_EQ(10, output);
}

TEST_F(TestCInterface, TestGetBlockSize) {
    EXPECT_EQ(-1, hdfsGetDefaultBlockSize(NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GT(hdfsGetDefaultBlockSize(fs), 0);
}

TEST_F(TestCInterface, TestGetCapacity) {
    EXPECT_EQ(-1, hdfsGetCapacity(NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GT(hdfsGetCapacity(fs), 0);
}

TEST_F(TestCInterface, TestGetUsed) {
    EXPECT_EQ(-1, hdfsGetUsed(NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GE(hdfsGetUsed(fs), 0);
}

TEST_F(TestCInterface, TestAvailable) {
    hdfsFile out = NULL, in = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/TestAvailable_InvalidInput", O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(NULL != out);
    in = hdfsOpenFile(fs, BASE_DIR"/TestAvailable_InvalidInput", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != in);
    EXPECT_EQ(-1, hdfsAvailable(NULL, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsAvailable(fs, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsAvailable(NULL, in));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsAvailable(fs, out));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GE(hdfsAvailable(fs, in), 0);
    ASSERT_EQ(0, hdfsCloseFile(fs, in));
    ASSERT_EQ(0, hdfsCloseFile(fs, out));
}

TEST_F(TestCInterface, TestOpenFile_InvalidInput) {
    hdfsFile file = NULL;
    //test invalid input
    file = hdfsOpenFile(NULL, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    file = hdfsOpenFile(fs, NULL, O_WRONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    file = hdfsOpenFile(fs, "", O_WRONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    //test O_RDWR flag
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_RDWR, 0, 0, 0);
    EXPECT_TRUE(file == NULL && ENOTSUP == errno);
    //test O_EXCL | O_CREATE flag
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_EXCL | O_CREAT, 0, 0, 0);
    EXPECT_TRUE(file == NULL && ENOTSUP == errno);
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, -1, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    //test invalid block size
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 1);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    //open not exist file
    file = hdfsOpenFile(fs, BASE_DIR"/notExist", O_RDONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && ENOENT == errno);
}

TEST_F(TestCInterface, TestOpenFile_Success) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    //crate a file
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_CREAT, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //append to a file
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //overwrite a file
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //remove file
    ASSERT_EQ(hdfsDelete(fs, BASE_DIR"/testOpenFile", true), 0);
    //create a new file with block size, replica size
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 1, 1024);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    info = hdfsGetPathInfo(fs, BASE_DIR"/testOpenFile");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(1024 == info->mBlockSize && 0 == info->mSize && 1 == info->mReplication);
    hdfsFreeFileInfo(info, 1);
    //open for read
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //test open a file for write, which has been opened for write, should success
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    hdfsFile another = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 1, 1024);
    ASSERT_TRUE(another != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    EXPECT_EQ(hdfsCloseFile(fs, another), 0);
    //test open a file for append, which has been opened for write, should fail.
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    another = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY | O_APPEND, 0, 0, 0);
    EXPECT_TRUE(another == NULL && EBUSY == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

TEST_F(TestCInterface, TestFileExist_InvalidInput) {
    int err;
    //test invalid input
    err = hdfsExists(NULL, BASE_DIR"/notExist");
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    err = hdfsExists(fs, NULL);
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    //test file which does not exist
    err = hdfsExists(fs, BASE_DIR"/notExist");
    EXPECT_EQ(err, -1);
}

TEST_F(TestCInterface, TestFileExist_Success) {
    hdfsFile file = NULL;
    //create a file and test
    file = hdfsOpenFile(fs, BASE_DIR"/testExists", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    EXPECT_EQ(hdfsExists(fs, BASE_DIR"/testExists"), 0);
}

TEST_F(TestCInterface, TestFileClose_InvalidInput) {
    int retval;
    hdfsFile file;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileClose", O_WRONLY, 0, 0, 0);
    //test invalid input
    retval = hdfsCloseFile(NULL, file);
    EXPECT_TRUE(retval == -1 && EINVAL == errno);
    EXPECT_EQ(hdfsCloseFile(fs, NULL), 0);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

TEST_F(TestCInterface, TestFileClose_Success) {
    int retval;
    hdfsFile file;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileClose", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    //test close file
    retval = hdfsCloseFile(fs, file);
    EXPECT_EQ(retval, 0);
    EXPECT_EQ(hdfsCloseFile(fs, NULL), 0);
}

TEST_F(TestCInterface, TestFileSeek_InvalidInput) {
    int err;
    hdfsFile file = NULL;
    //test invalid input
    err = hdfsSeek(NULL, file, 1);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsSeek(fs, NULL, 1);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_TRUE(4 == hdfsWrite(fs, file, "abcd", 4));
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    //seek over eof
    err = hdfsSeek(fs, file, 100);
    EXPECT_TRUE(-1 == err && EOVERFLOW == errno);
    //test seek a file which is opened for write
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_WRONLY | O_APPEND, 0, 0,
                        0);
    ASSERT_TRUE(NULL != file);
    err = hdfsSeek(fs, file, 1);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

static void TestSeek(hdfsFS fs) {
    int err;
    hdfsFile file = NULL;
    int blockSize = 1024 * 1024;
    std::vector<char> buffer(8 * 1024);
    ASSERT_TRUE(CreateFile(fs, BASE_DIR"/testFileSeek", blockSize, blockSize * 21));
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    //seek to eof, we do expect to return success
    err = hdfsSeek(fs, file, blockSize * 21);
    EXPECT_EQ(0, err);
    //seek to 0
    err = hdfsSeek(fs, file, 0);
    EXPECT_EQ(0, err);
    EXPECT_EQ(0, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), 0));
    //seek to current position
    err = hdfsSeek(fs, file, buffer.size());
    EXPECT_EQ(0, err);
    EXPECT_EQ(buffer.size(), hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), buffer.size()));
    //seek to next block
    err = hdfsSeek(fs, file, blockSize);
    EXPECT_EQ(0, err);
    EXPECT_EQ(blockSize, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), blockSize));
    //seek to next 20 block
    err = hdfsSeek(fs, file, blockSize * 20);
    EXPECT_EQ(0, err);
    EXPECT_EQ(blockSize * 20, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), blockSize * 20));
    //seek back to second block
    err = hdfsSeek(fs, file, blockSize);
    EXPECT_EQ(0, err);
    EXPECT_EQ(blockSize, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), blockSize));
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

TEST_F(TestCInterface, TestFileSeek_Success) {
    TestSeek(fs);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    hdfsBuilderConfSetStr(bld, "dfs.client.read.shortcircuit", "false");
    hdfsBuilderSetForceNewInstance(bld);
    hdfsFS newfs = hdfsBuilderConnect(bld);
    hdfsFreeBuilder(bld);
    ASSERT_TRUE(newfs != NULL);
    TestSeek(newfs);
    hdfsDisconnect(newfs);
}

TEST_F(TestCInterface, TestFileTell_InvalidInput) {
    int64_t err;
    hdfsFile out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testFileTell", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    //test invalid input
    err = hdfsTell(NULL, out);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsTell(fs, NULL);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    hdfsCloseFile(fs, out);
}

TEST_F(TestCInterface, TestFileTell_Success) {
    hdfsFile in, out;
    int batch;
    size_t offset = 0;
    int64_t todo, fileSize = 21 * 1024 * 1024;
    std::vector<char> buffer(8 * 1024);
    out = hdfsOpenFile(fs, BASE_DIR"/testFileTell", O_WRONLY, 0, 0, 1024 * 1024);
    ASSERT_TRUE(out != NULL);
    ASSERT_EQ(0, hdfsTell(fs, out));
    srand(0);
    todo = fileSize;

    while (todo > 0) {
        batch = (rand() % (buffer.size() - 1)) + 1;
        batch = batch < todo ? batch : todo;
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        ASSERT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
        todo -= batch;
        offset += batch;
        ASSERT_EQ(fileSize - todo, hdfsTell(fs, out));
    }

    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    in = hdfsOpenFile(fs, BASE_DIR"/testFileTell", O_RDONLY, 0, 0, 0);
    EXPECT_TRUE(NULL != in);
    EXPECT_EQ(0, hdfsTell(fs, in));
    offset = 0;
    todo = fileSize;

    while (todo > 0) {
        batch = (rand() % (buffer.size() - 1)) + 1;
        batch = batch < todo ? batch : todo;
        batch = hdfsRead(fs, in, &buffer[0], batch);
        ASSERT_TRUE(batch > 0);
        todo -= batch;
        EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], batch, offset));
        offset += batch;
        ASSERT_EQ(fileSize - todo, hdfsTell(fs, in));
    }

    EXPECT_EQ(0, hdfsCloseFile(fs, in));
}

TEST_F(TestCInterface, TestDelete_InvalidInput) {
    int err;
    //test invalid input
    err = hdfsDelete(NULL, BASE_DIR"/testFileDelete", 0);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsDelete(fs, NULL, 0);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsDelete(fs, "", 0);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
}

TEST_F(TestCInterface, TestDelete_Success) {
    hdfsFile file = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileDelete", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsCreateDirectory(fs, BASE_DIR"/testFileDeleteDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testFileDeleteDir/testFileDelete",
                        O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //delete file
    err = hdfsDelete(fs, BASE_DIR"/testFileDelete", 0);
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileDelete");
    EXPECT_NE(err, 0);
    //delete directory
    err = hdfsDelete(fs, BASE_DIR"/testFileDeleteDir", 1);
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileDeleteDir/testFileDelete");
    EXPECT_NE(err, 0);
    err = hdfsExists(fs, BASE_DIR"/testFileDeleteDir");
    EXPECT_NE(err, 0);
}

TEST_F(TestCInterface, TestRename_InvalidInput) {
    hdfsFile file = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileRename", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsCreateDirectory(fs, BASE_DIR"/testFileRenameDir");
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsRename(NULL, BASE_DIR"/testFileRename",
                     BASE_DIR"/testFileRenameNew");
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, NULL, BASE_DIR"/testFileRenameNew");
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, "", BASE_DIR"/testFileRenameNew");
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, BASE_DIR"/testFileRename", NULL);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, BASE_DIR"/testFileRename", "");
    EXPECT_TRUE(0 != err && EINVAL == errno);
}

TEST_F(TestCInterface, TestRename_Success) {
    hdfsFile file = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileRename", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsCreateDirectory(fs, BASE_DIR"/testFileRenameDir");
    EXPECT_EQ(0, err);
    //rename a file
    err = hdfsRename(fs, BASE_DIR"/testFileRename",
                     BASE_DIR"/testFileRenameNew");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileRenameNew");
    EXPECT_EQ(0, err);
    //rename a directory
    err = hdfsRename(fs, BASE_DIR"/testFileRenameDir",
                     BASE_DIR"/testFileRenameDirNew");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileRenameDirNew");
    EXPECT_EQ(0, err);
}

TEST_F(TestCInterface, TestGetWorkingDirectory_InvalidInput) {
    char * ret, buffer[1024];
    //test invalid input
    ret = hdfsGetWorkingDirectory(NULL, buffer, sizeof(buffer));
    EXPECT_TRUE(ret == 0 && EINVAL == errno);
    ret = hdfsGetWorkingDirectory(fs, NULL, sizeof(buffer));
    EXPECT_TRUE(ret == 0 && EINVAL == errno);
    ret = hdfsGetWorkingDirectory(fs, buffer, 0);
    EXPECT_TRUE(ret == 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestGetWorkingDirectory_Success) {
    char * ret, buffer[1024];
    ret = hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer));
    EXPECT_TRUE(ret != NULL);
    EXPECT_STREQ("/user/" USER, buffer);
}

TEST_F(TestCInterface, TestSetWorkingDirectory_InvalidInput) {
    int err;
    //test invalid input
    err = hdfsSetWorkingDirectory(NULL, BASE_DIR);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetWorkingDirectory(fs, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetWorkingDirectory(fs, "");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestSetWorkingDirectory_Success) {
    int err;
    char * ret, target[1024], buffer[1024];
    ret = hdfsGetWorkingDirectory(fs, target, sizeof(target));
    ASSERT_TRUE(ret != NULL);
    strcat(target, "/./" BASE_DIR);
    err = hdfsSetWorkingDirectory(fs, target);
    EXPECT_EQ(0, err);
    ret = hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer));
    EXPECT_TRUE(ret != NULL && strcmp(buffer, target) == 0);
    err = hdfsCreateDirectory(fs, "testCreateDir");
    EXPECT_EQ(0, err);
    strcat(target, "testCreateDir");
    err = hdfsExists(fs, target);
    EXPECT_EQ(err, 0);
}

TEST_F(TestCInterface, TestCreateDir_InvalidInput) {
    int err = 0;
    //test invalid input
    err = hdfsCreateDirectory(NULL, BASE_DIR"/testCreateDir");
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    err = hdfsCreateDirectory(fs, NULL);
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    err = hdfsCreateDirectory(fs, "");
    EXPECT_TRUE(err == -1 && EINVAL == errno);
}

TEST_F(TestCInterface, TestCreateDir_Success) {
    int err = 0;
    err = hdfsCreateDirectory(fs, BASE_DIR"/testCreateDir");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testCreateDir");
    EXPECT_EQ(0, err);
    err = hdfsCreateDirectory(fs, BASE_DIR"/testCreate  Dir");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testCreate  Dir");
    EXPECT_EQ(0, err);
}

TEST_F(TestCInterface, TestSetReplication_InvalidInput) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testSetReplication", O_WRONLY, 0, 1, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //test invalid input
    err = hdfsSetReplication(NULL, BASE_DIR"/testSetReplication", 2);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetReplication(fs, NULL, 2);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetReplication(fs, "", 2);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetReplication(fs, BASE_DIR"/testSetReplication", 0);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestSetReplication_Success) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testSetReplication", O_WRONLY, 0, 1, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsSetReplication(fs, BASE_DIR"/testSetReplication", 2);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testSetReplication");
    EXPECT_TRUE(info != NULL);
    EXPECT_EQ(info->mReplication, 2);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestListDirectory_InvalidInput) {
    int num;
    hdfsFileInfo * info = NULL;
    //test invalid input
    info = hdfsListDirectory(NULL, BASE_DIR, &num);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, NULL, &num);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, "", &num);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, BASE_DIR, NULL);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, BASE_DIR"NOTEXIST", &num);
    EXPECT_TRUE(info == NULL);
    EXPECT_EQ(ENOENT, errno);
}

TEST_F(TestCInterface, TestListDirectory_Success) {
    hdfsFileInfo * info;
    int num, numFiles = 5000, numDirs = 5000;
    //empty dir
    ASSERT_EQ(0, hdfsCreateDirectory(fs, BASE_DIR"/empty"));
    info = hdfsListDirectory(fs, BASE_DIR"/empty", &num);
    ASSERT_TRUE(NULL != info);
    ASSERT_EQ(0, num);
    hdfsFreeFileInfo(info, num);
    ASSERT_EQ(0, hdfsCreateDirectory(fs, BASE_DIR"/testListDirectoryDir"));
    char path[1024];

    for (int i = 0 ; i < numFiles ; ++i) {
        sprintf(path, "%s/File%d", BASE_DIR"/testListDirectoryDir", i);
        ASSERT_TRUE(CreateFile(fs, path, 0, 0));
    }

    for (int i = 0 ; i < numDirs ; ++i) {
        sprintf(path, "%s/Dir%d", BASE_DIR"/testListDirectoryDir", i);
        ASSERT_EQ(0, hdfsCreateDirectory(fs, path));
    }

    info = hdfsListDirectory(fs, BASE_DIR "/testListDirectoryDir", &num);
    ASSERT_TRUE(info != NULL);
    EXPECT_EQ(num, numFiles + numDirs);
    hdfsFreeFileInfo(info, num);
    info = hdfsListDirectory(fs, "/", &num);
    ASSERT_TRUE(info != NULL);
    hdfsFreeFileInfo(info, num);
}

TEST_F(TestCInterface, TestGetPathInfo_InvalidInput) {
    hdfsFileInfo * info = NULL;
    //test invalid input
    info = hdfsGetPathInfo(NULL, BASE_DIR"/testGetPathInfo");
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsGetPathInfo(fs, NULL);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsGetPathInfo(fs, "");
    EXPECT_TRUE(info == NULL && EINVAL == errno);
}

static std::string FileName(const std::string & path) {
    size_t pos = path.find_last_of('/');

    if (pos != path.npos && pos != path.length() - 1) {
      return path.c_str() + pos + 1;
    } else {
      return path;
    }
}

TEST_F(TestCInterface, TestGetPathInfo_Success) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    file = hdfsOpenFile(fs, BASE_DIR"/testGetPathInfo", O_WRONLY, 0, 1, 1024);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    EXPECT_EQ(0, hdfsCreateDirectory(fs, BASE_DIR "/testGetDirInfo"));
    info = hdfsGetPathInfo(fs, BASE_DIR"/testGetPathInfo");
    ASSERT_TRUE(info != NULL);
    EXPECT_EQ(1024, info->mBlockSize);
    EXPECT_STREQ("supergroup", info->mGroup);
    EXPECT_EQ(kObjectKindFile, info->mKind);
    //EXPECT_TRUE(info->mLastAccess >= 0);
    //EXPECT_TRUE(info->mLastMod >= 0);
    //&& 0666 == info->mPermissions
    EXPECT_EQ(1, info->mReplication);
    EXPECT_EQ(0, info->mSize);
    EXPECT_STREQ("testGetPathInfo", FileName(info->mName).c_str());
    EXPECT_STREQ(USER, info->mOwner);
    hdfsFreeFileInfo(info, 1);
    info = hdfsGetPathInfo(fs, BASE_DIR "/testGetDirInfo");
    ASSERT_TRUE(info != NULL);
    EXPECT_EQ(0, info->mBlockSize);
    EXPECT_STREQ("supergroup", info->mGroup);
    EXPECT_EQ(kObjectKindDirectory, info->mKind);
    EXPECT_TRUE(info->mLastAccess >= 0);
    //EXPECT_TRUE(info->mLastMod >= 0);
    //&& 0744 == info->mPermissions
    EXPECT_EQ(0, info->mReplication);
    EXPECT_EQ(0, info->mSize);
    EXPECT_STREQ("testGetDirInfo", FileName(info->mName).c_str());
    EXPECT_STREQ(USER, info->mOwner);
    hdfsFreeFileInfo(info, 1);
    info = hdfsGetPathInfo(fs, "/");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(info->mKind == kObjectKindDirectory && strcmp(info->mName, "/") == 0);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestChown_InvalidInput) {
    int err = 0;
    hdfsFile file = NULL;
    file = hdfsOpenFile(fs, BASE_DIR"/testChwonFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsChown(NULL, BASE_DIR"/testChwonFile", "testChown", "testChown");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, NULL, "testChown", "testChown");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, "", "testChown", "testChown");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", NULL, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", "", NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", NULL, "");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", "", "");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestChown_Success) {
    int err = 0;
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    //test chown on file
    file = hdfsOpenFile(fs, BASE_DIR"testChownFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    err = hdfsChown(superfs, BASE_DIR"testChownFile", "testChown", "testChown");
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(superfs, BASE_DIR"testChownFile");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(strcmp(info->mOwner, "testChown") == 0
                && strcmp(info->mGroup, "testChown") == 0);
    hdfsFreeFileInfo(info, 1);
    //test chown on directory
    err = hdfsCreateDirectory(fs, BASE_DIR"testChwonDir");
    EXPECT_EQ(0, err);
    err = hdfsChown(superfs, BASE_DIR"testChwonDir", "testChown", "testChown");
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(superfs, BASE_DIR"testChwonDir");
    EXPECT_TRUE(info != NULL);
    EXPECT_TRUE(strcmp(info->mOwner, "testChown") == 0
                && strcmp(info->mGroup, "testChown") == 0);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestChmod_InvalidInput) {
    int err = 0;
    hdfsFile file = NULL;
    err = hdfsCreateDirectory(fs, BASE_DIR"/testChmodDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testChmodFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsChmod(NULL, BASE_DIR"/testChmodFile", 0777);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChmod(fs, NULL, 0777);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChmod(fs, "", 0777);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChmod(fs, BASE_DIR"/testChmodFile", 02000);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestChmod_Success) {
    int err = 0;
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    err = hdfsCreateDirectory(fs, BASE_DIR"/testChmodDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testChmodFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    err = hdfsChmod(fs, BASE_DIR"/testChmodFile", 0777);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testChmodFile");
    ASSERT_TRUE(info != NULL);
    //not executable
    EXPECT_TRUE(info->mPermissions == 0777);
    hdfsFreeFileInfo(info, 1);
    err = hdfsChmod(fs, BASE_DIR"/testChmodDir", 0777);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testChmodDir");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(info->mPermissions == 0777);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestUtime_InvalidInput) {
    int err = 0;
    hdfsFile file = NULL;
    time_t now = time(NULL);
    err = hdfsCreateDirectory(fs, BASE_DIR"/testUtimeDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testUtimeFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsUtime(NULL, BASE_DIR"/testUtimeFile", now, now);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsUtime(fs, NULL, now, now);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsUtime(fs, "", now, now);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestUtime_Success) {
    int err = 0;
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    time_t now = time(NULL);
    err = hdfsCreateDirectory(fs, BASE_DIR"/testUtimeDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testUtimeFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    err = hdfsUtime(fs, BASE_DIR"/testUtimeDir", now, now);
    EXPECT_EQ(0, err);
    err = hdfsUtime(fs, BASE_DIR"/testUtimeFile", now, now);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testUtimeFile");
    ASSERT_TRUE(info);
    EXPECT_TRUE(now / 1000 == info->mLastAccess && info->mLastAccess);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestRead_InvalidInput) {
    int err;
    char buf[10240];
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testRead", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    //test invalid input
    err = hdfsRead(NULL, in, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, NULL, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, out, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, in, NULL, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, in, buf, 0);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    hdfsCloseFile(fs, out);
}

static void TestRead(hdfsFS fs, int readSize, int64_t blockSize,
                     int64_t fileSize) {
    int batch, done;
    size_t offset = 0;
    int64_t todo = fileSize;
    hdfsFile in = NULL;
    std::vector<char> buf(readSize + 100);
    ASSERT_TRUE(CreateFile(fs, BASE_DIR"/testRead", blockSize, fileSize));
    in = hdfsOpenFile(fs, BASE_DIR"/testRead", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);

    while (todo > 0) {
        batch = todo < readSize ? todo : readSize;
        done = hdfsRead(fs, in, &buf[0], batch);
        ASSERT_GT(done, 0);
        EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
        offset = offset + done;
        todo -= done;

        if (todo > 0) {
            batch = todo < readSize ? todo : readSize - 100;
            done = hdfsRead(fs, in, &buf[0], batch);
            ASSERT_GT(done, 0);
            EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
            offset = offset + done;
            todo -= done;
        }

        if (todo > 0) {
            batch = todo < readSize ? todo : readSize + 100;
            done = hdfsRead(fs, in, &buf[0], batch);
            ASSERT_GT(done, 0);
            EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
            offset = offset + done;
            todo -= done;
        }
    }

    EXPECT_EQ(0, hdfsRead(fs, in, &buf[0], 1));
    EXPECT_EQ(0, hdfsRead(fs, in, &buf[0], 1));
    hdfsCloseFile(fs, in);
}

TEST_F(TestCInterface, TestRead_Success) {
    TestRead(fs, 1024, 1024, 21 * 1024);
    TestRead(fs, 8 * 1024, 1024 * 1024, 21 * 1024 * 1024);
}

static void TestPread(hdfsFS fs, int readSize, int64_t blockSize,
                     int64_t fileSize, int64_t offset) {
    std::vector<char> buf(readSize);
    hdfsFile in = NULL;
    ASSERT_TRUE(CreateFile(fs, BASE_DIR"/testPread", blockSize, fileSize));
    in = hdfsOpenFile(fs, BASE_DIR"/testPread", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);

    int64_t todo = fileSize - offset;
    while (todo > 0) {
        int64_t done = hdfsPread(fs, in, offset, &buf[0], std::min((int64_t) readSize, todo));
        ASSERT_GT(done, 0);
        ASSERT_EQ(hdfsTell(fs, in), 0);
        EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
        offset += done;
        todo -= done;
    }

    EXPECT_EQ(0, hdfsPread(fs, in, offset, &buf[0], 1));
    EXPECT_EQ(0, hdfsPread(fs, in, offset, &buf[0], 1));
    hdfsCloseFile(fs, in);
}

TEST_F(TestCInterface, TestPread_Success) {
    TestPread(fs, 1024, 1024, 21 * 1024, 0);
    TestPread(fs, 1024, 1024, 21 * 1024, 13);
}

TEST_F(TestCInterface, TestWrite_InvalidInput) {
    int err;
    char buf[10240];
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testWrite1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    out = hdfsOpenFile(fs, BASE_DIR"/testWrite", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    in = hdfsOpenFile(fs, BASE_DIR"/testWrite1", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);
    //test invalid input
    err = hdfsWrite(NULL, out, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsWrite(fs, NULL, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsWrite(fs, in, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsCloseFile(fs, in);
    EXPECT_EQ(0, err);
    err = hdfsWrite(fs, out, NULL, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsWrite(fs, out, buf, 0);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
}

static void TestWrite(hdfsFS fs, int writeSize, int64_t blockSize,
                      int64_t fileSize) {
    hdfsFile out;
    size_t offset = 0;
    int64_t batch, todo = fileSize;
    std::vector<char> buffer(writeSize + 123);
    out = hdfsOpenFile(fs, BASE_DIR "/testWrite1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);

    while (todo > 0) {
        batch = todo < writeSize ? todo : writeSize;
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        EXPECT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
        todo -= batch;
        offset += batch;

        if (todo > 0) {
            batch = todo < writeSize - 123 ? todo : writeSize - 123;
            Hdfs::FillBuffer(&buffer[0], batch, offset);
            EXPECT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
            todo -= batch;
            offset += batch;
        }

        if (todo > 0) {
            batch = todo < writeSize + 123 ? todo : writeSize + 123;
            Hdfs::FillBuffer(&buffer[0], batch, offset);
            EXPECT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
            todo -= batch;
            offset += batch;
        }
    }

    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR "/testWrite1", fileSize, 0));
}

TEST_F(TestCInterface, TestWrite_Success) {
    TestWrite(fs, 512, 1024 * 1024, 21 * 1024 * 1024);
    TestWrite(fs, 654, 1024 * 1024, 21 * 1024 * 1024);
    TestWrite(fs, 64 * 1024, 1024 * 1024, 21 * 1024 * 1024);
    TestWrite(fs, 1024 * 1024, 1024 * 1024, 21 * 1024 * 1024);
}

TEST_F(TestCInterface, TestHFlush_InvalidInput) {
    int err;
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush2", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    in = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);
    //test invalid input
    err = hdfsHFlush(NULL, out);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsHFlush(fs, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsHFlush(fs, in);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    EXPECT_EQ(0, hdfsCloseFile(fs, in));
}

TEST_F(TestCInterface, TestSync_InvalidInput) {
    int err;
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush2", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    in = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);
    //test invalid input
    err = hdfsHSync(NULL, out);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsHSync(fs, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsHSync(fs, in);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    EXPECT_EQ(0, hdfsCloseFile(fs, in));
}

static void TestHFlushAndSync(hdfsFS fs, hdfsFile file, const char * path, int64_t blockSize, bool sync) {
    hdfsFile in;
    size_t offset = 0;
    int64_t batch, fileSize, todo;
    fileSize = todo = blockSize * 3 + 123;
    std::vector<char> buffer;
    in = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);
    enum TestWriteSize {
        InChunk, ChunkBoundary, InPackage, PackageBoundary, RandomSize
    };
    TestWriteSize buferSizeTarget = InChunk;

    while (todo > 0) {
        switch (buferSizeTarget) {
        case InChunk:
            buffer.resize(123);
            buferSizeTarget = ChunkBoundary;
            break;

        case ChunkBoundary:
            buffer.resize(512 - (fileSize - todo) % 512);
            buferSizeTarget = InPackage;
            break;

        case InPackage:
            buffer.resize(512 + 123);
            buferSizeTarget = PackageBoundary;
            break;

        case PackageBoundary:
            buffer.resize(512 * 128 - (fileSize - todo) % (512 * 128));
            buferSizeTarget = RandomSize;
            break;

        default:
            buffer.resize(blockSize - 1);
            break;
        }

        batch = todo < static_cast<int>(buffer.size()) ? todo : buffer.size();
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        EXPECT_EQ(batch, hdfsWrite(fs, file, &buffer[0], batch));

        if (sync) {
            EXPECT_EQ(0, hdfsHSync(fs, file));
        } else {
            EXPECT_EQ(0, hdfsHFlush(fs, file));
        }

        EXPECT_TRUE(ReadFully(fs, in, &buffer[0], batch));
        EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], batch, offset));
        todo -= batch;
        offset += batch;
    }

    hdfsCloseFile(fs, in);
    EXPECT_TRUE(CheckFileContent(fs, path, fileSize, 0));
}

TEST_F(TestCInterface, TestFlushAndSync_Success) {
    hdfsFile out;
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY, 0, 0, 1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, false);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY | O_SYNC, 0, 0,
                       1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, false);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY, 0, 0, 1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, true);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY | O_SYNC, 0, 0,
                       1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, true);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
}

TEST_F(TestCInterface, TestTruncate_InvalidInput) {
    int err;
    int shouldWait;
    std::vector<char> buf(10240);
    int bufferSize = buf.size();
    hdfsFile out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY, 0, 0, 2048);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], bufferSize, 0);
    EXPECT_TRUE(bufferSize == hdfsWrite(fs, out, &buf[0], bufferSize));
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsTruncate(NULL, BASE_DIR"/testTruncate", 1, &shouldWait);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsTruncate(fs, NULL, 1, &shouldWait);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsTruncate(fs, "", 1, &shouldWait);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsTruncate(fs, "NOTEXIST", 1, &shouldWait);
    EXPECT_TRUE(-1 == err && (ENOENT == errno || ENOTSUP == errno));
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", bufferSize + 1, &shouldWait);
    EXPECT_TRUE(0 != err && (EINVAL == errno || ENOTSUP == errno));
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", -1, &shouldWait);
    EXPECT_TRUE(0 != err && (EINVAL == errno || ENOTSUP == errno));
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0,
                       0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", 1, &shouldWait);
    EXPECT_TRUE(0 != err && (EBUSY == errno || ENOTSUP == errno));
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
}

TEST_F(TestCInterface, TestTruncate_Success) {
    int shouldWait;
    int err = hdfsTruncate(fs, "NOTEXIST", 1, &shouldWait);

    if (-1 == err && ENOTSUP == errno) {
        return;
    }

    size_t fileLength = 0;
    int bufferSize = 10240, blockSize = 2048;
    std::vector<char> buf(bufferSize);
    hdfsFile out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY, 0, 0, blockSize);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    //test truncate to the end of file
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", buf.size(), &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_FALSE(shouldWait);
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0, 0,
                       0);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
    //test truncate to the block boundary
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", blockSize, &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_FALSE(shouldWait);
    EXPECT_EQ(blockSize, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", blockSize, 0));
    fileLength = blockSize;
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0, 0,
                       0);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
    //test truncate to 1
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", 1, &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(shouldWait);
    fileLength = 1;

    do {
        out = hdfsOpenFile(fs, BASE_DIR "/testTruncate", O_WRONLY | O_APPEND, 0,
                           0, 0);
    } while (out == NULL && errno == EBUSY);

    ASSERT_TRUE(out != NULL);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", 1, 0));
    Hdfs::FillBuffer(&buf[0], buf.size() - 1, fileLength);
    EXPECT_EQ(buf.size() - 1,  hdfsWrite(fs, out, &buf[0], buf.size() - 1));
    fileLength += buf.size() - 1;
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
    //test truncate to 0
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", 0, &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_FALSE(shouldWait);
    fileLength = 0;
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0, 0,
                       0);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
}

static void TestAppend(hdfsFS fs, int64_t blockSize) {
    int batch;
    hdfsFile out = NULL;
    size_t offset = 0;
    const int chunkSize = 512;
    int64_t fileLength = 0;
    std::vector<char> buffer(8 * 1024 + 123);
    //create a new empty file
    out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY, 0, 0, blockSize);
    EXPECT_EQ(0, hdfsCloseFile(fs, out));

    for (int i = 0; i < 21; ++i) {
        //append to a new empty block
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY | O_APPEND | O_SYNC, 0, 0,
                           blockSize);
        ASSERT_TRUE(out != NULL);
        //write half chunk
        Hdfs::FillBuffer(&buffer[0], chunkSize / 2, offset);
        EXPECT_EQ(chunkSize / 2, hdfsWrite(fs, out, &buffer[0], chunkSize / 2));
        offset += chunkSize / 2;
        fileLength += chunkSize / 2;
        ASSERT_EQ(0, hdfsCloseFile(fs, out));
        //append another half chunk
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY | O_APPEND | O_SYNC, 0, 0,
                           blockSize);
        ASSERT_TRUE(out != NULL);
        Hdfs::FillBuffer(&buffer[0], chunkSize / 2, offset);
        EXPECT_EQ(chunkSize / 2, hdfsWrite(fs, out, &buffer[0], chunkSize / 2));
        offset += chunkSize / 2;
        fileLength += chunkSize / 2;
        ASSERT_EQ(0, hdfsCloseFile(fs, out));
        //append to a full block
        int64_t todo = blockSize - chunkSize;
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY | O_APPEND, 0, 0,
                           blockSize);
        ASSERT_TRUE(out != NULL);

        while (todo > 0) {
            batch = todo < static_cast<int>(buffer.size()) ?
                    todo : buffer.size();
            Hdfs::FillBuffer(&buffer[0], batch, offset);
            ASSERT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
            todo -= batch;
            offset += batch;
            fileLength += batch;
        }

        ASSERT_EQ(0, hdfsCloseFile(fs, out));
    }

    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testAppend", fileLength, 0));
}

TEST_F(TestCInterface, TestAppend_Success) {
    TestAppend(fs, 32 * 1024);
    TestAppend(fs, 1024 * 1024);
}

TEST_F(TestCInterface, TestAppend2) {
    hdfsFile out = NULL;
    std::vector<char> buf(10240);
    LOG(INFO, "==TestAppend2==");

    for (int i = 0; i < 100; i++) {
        LOG(INFO, "Loop in testAppend2 %d", i);
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend2", O_WRONLY | O_APPEND, 0, 0, 0);
        ASSERT_TRUE(NULL != out);
        Hdfs::FillBuffer(&buf[0], buf.size(), 0);
        ASSERT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
        ASSERT_EQ(0, hdfsCloseFile(fs, out));
    }

    LOG(INFO, "==TestAppend2 done==");
}

TEST_F(TestCInterface, TestOpenForReadWrite) {
    hdfsFile out = NULL, in = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/TestOpenForRead", O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(NULL != out);
    ASSERT_FALSE(hdfsFileIsOpenForRead(out));
    ASSERT_TRUE(hdfsFileIsOpenForWrite(out));
    ASSERT_EQ(0, hdfsCloseFile(fs, out));
    in = hdfsOpenFile(fs, BASE_DIR"/TestOpenForRead", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != in);
    ASSERT_TRUE(hdfsFileIsOpenForRead(in));
    ASSERT_FALSE(hdfsFileIsOpenForWrite(in));
    ASSERT_EQ(0, hdfsCloseFile(fs, in));
    ASSERT_FALSE(hdfsFileIsOpenForRead(NULL));
    ASSERT_FALSE(hdfsFileIsOpenForWrite(NULL));
}

TEST_F(TestCInterface, TestGetHANamenode) {
    int size;
    Namenode * namenodes = NULL;
    EXPECT_TRUE(NULL == hdfsGetHANamenodes(NULL, &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodes("", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodes("phdcluster", NULL));
    EXPECT_TRUE(errno == EINVAL);
    ASSERT_TRUE(NULL != (namenodes = hdfsGetHANamenodes("phdcluster", &size)));
    ASSERT_EQ(2, size);
    EXPECT_STREQ("mdw:8020", namenodes[0].rpc_addr);
    EXPECT_STREQ("mdw:50070", namenodes[0].http_addr);
    EXPECT_STREQ("smdw:8020", namenodes[1].rpc_addr);
    EXPECT_STREQ("smdw:50070", namenodes[1].http_addr);
    EXPECT_NO_THROW(hdfsFreeNamenodeInformation(namenodes, size));
}

TEST_F(TestCInterface, TestGetBlockFileLocations_Failure) {
    int size;
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(NULL, NULL, 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, NULL, 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "", 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "NOTEXIST", 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "NOTEXIST", 0, 1, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "NOTEXIST", 0, 1, &size));
    EXPECT_TRUE(errno == ENOENT);
}


TEST_F(TestCInterface, TestGetBlockFileLocations_Success) {
    int size;
    BlockLocation * bl;
    hdfsFile out = NULL;
    std::vector<char> buffer(1025);
    out = hdfsOpenFile(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", O_WRONLY, 0, 0, 1024);
    ASSERT_TRUE(NULL != out);
    hdfsCloseFile(fs, out);
    EXPECT_TRUE(NULL != (bl = hdfsGetFileBlockLocations(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", 0, 1, &size)));
    EXPECT_EQ(0, size);
    hdfsFreeFileBlockLocations(bl, size);
    out = hdfsOpenFile(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", O_WRONLY, 0, 0, 1024);
    ASSERT_TRUE(NULL != out);
    ASSERT_TRUE(buffer.size() == hdfsWrite(fs, out, &buffer[0], buffer.size()));
    ASSERT_TRUE(0 == hdfsHSync(fs, out));
    EXPECT_TRUE(NULL != (bl = hdfsGetFileBlockLocations(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", 0, buffer.size(), &size)));
    EXPECT_EQ(2, size);
    EXPECT_FALSE(bl[0].corrupt);
    EXPECT_FALSE(bl[1].corrupt);
    EXPECT_EQ(0, bl[0].offset);
    EXPECT_EQ(1024, bl[1].offset);
    EXPECT_EQ(1024, bl[0].length);
    hdfsFreeFileBlockLocations(bl, size);
    EXPECT_TRUE(NULL != (bl = hdfsGetFileBlockLocations(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", buffer.size(), 1, &size)));
    EXPECT_EQ(0, size);
    hdfsFreeFileBlockLocations(bl, size);
    hdfsCloseFile(fs, out);
}

TEST_F(TestCInterface, TestGetHosts_Failure) {
    EXPECT_TRUE(NULL == hdfsGetHosts(NULL, NULL, 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, NULL, 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "", 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "NOTEXIST", 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "NOTEXIST", -1, 1));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "NOTEXIST", 0, 1));
    EXPECT_TRUE(errno == ENOENT);
}

TEST_F(TestCInterface, TestGetHosts_Success) {
    char ***hosts;
    hdfsFile out = NULL;
    std::vector<char> buffer(1025);
    out = hdfsOpenFile(fs, BASE_DIR "/TestGetHosts_Success", O_WRONLY, 0, 0,
                       1024);
    ASSERT_TRUE(NULL != out);
    hdfsCloseFile(fs, out);
    hosts = hdfsGetHosts(fs, BASE_DIR "/TestGetHosts_Success", 0, 1);
    EXPECT_TRUE(NULL != hosts);
    EXPECT_TRUE(NULL == hosts[0]);
    hdfsFreeHosts(hosts);
    out = hdfsOpenFile(fs, BASE_DIR "/TestGetHosts_Success", O_WRONLY, 0, 0,
                       1024);
    ASSERT_TRUE(NULL != out);
    ASSERT_TRUE(buffer.size() == hdfsWrite(fs, out, &buffer[0], buffer.size()));
    ASSERT_TRUE(0 == hdfsHSync(fs, out));
    hosts =
        hdfsGetHosts(fs, BASE_DIR "/TestGetHosts_Success", 0, buffer.size());
    EXPECT_TRUE(NULL != hosts);
    EXPECT_TRUE(NULL != hosts[0]);
    EXPECT_TRUE(NULL != hosts[1]);
    EXPECT_TRUE(NULL == hosts[2]);
    hdfsFreeHosts(hosts);
    hdfsCloseFile(fs, out);
}

// test concurrent write to a same file
// expected:
//  At any point there can only be 1 writer.
//  This is enforced by requiring the writer to acquire leases.
TEST_F(TestCInterface, TestConcurrentWrite_Failure) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);

    const char *file_path = BASE_DIR "/concurrent_write";
    char buf[] = "1234";
    hdfsFile fout1 = hdfsOpenFile(fs, file_path, O_WRONLY | O_APPEND, 0, 0, 0);
    hdfsFile fout2 = hdfsOpenFile(fs, file_path, O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(fout2 == NULL); //must failed
    int rc = hdfsWrite(fs, fout1, buf, sizeof(buf)-1);
    ASSERT_TRUE(rc > 0);
    int retval = hdfsCloseFile(fs, fout1);
    ASSERT_TRUE(retval == 0);
}

/*all TDE read cases*/

//helper function
static void generate_file(const char *file_path, int file_size) {
    char buffer[1024];
    Hdfs::FillBuffer(buffer, sizeof(buffer), 0);

    int todo = file_size;
    FILE *f = fopen(file_path, "w");
    assert(f != NULL);
    while (todo > 0) {
        int batch = file_size;
        if (batch > sizeof(buffer))
            batch = sizeof(buffer);
        int rc = fwrite(buffer, 1, batch, f);
        //assert(rc == batch);
        todo -= rc;
    }
    fclose(f);
}

int diff_buf2filecontents(const char *file_path, const char *buf, int offset,
        int len) {
    char *local_buf = (char *) malloc(len);

    FILE *f = fopen(file_path, "r");
    assert(f != NULL);
    fseek(f, offset, SEEK_SET);

    int todo = len;
    int off = 0;
    while (todo > 0) {
        int rc = fread(local_buf + off, 1, todo, f);
        todo -= rc;
        off += rc;
    }
    fclose(f);

    int ret = strncmp(buf, local_buf, len);
    free(local_buf);
    return ret;
}

TEST(TestCInterfaceTDE, TestReadWithTDE_Basic_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);

    //create a normal file
    char cmd[128];
    const char *file_name = "tde_read_file";
    int file_size = 1024;
    generate_file(file_name, file_size);

    //put file to TDE encryption zone
    system("hadoop fs -rmr /TDEBasicRead");
    system("hadoop key create keytde4basicread");
    system("hadoop fs -mkdir /TDEBasicRead");
    ASSERT_EQ(0,
            hdfsCreateEncryptionZone(fs, "/TDEBasicRead", "keytde4basicread"));
    sprintf(cmd, "hdfs dfs -put `pwd`/%s /TDEBasicRead/", file_name);
    system(cmd);

    int offset = 0;
    int rc = 0;
    char buf[1024];
    int to_read = 5;
    char file_path[128];
    sprintf(file_path, "/TDEBasicRead/%s", file_name);
    hdfsFile fin = hdfsOpenFile(fs, file_path, O_RDONLY, 0, 0, 0);

    //case1: read from beginning
    offset = 0;
    rc = hdfsRead(fs, fin, buf, to_read);
    ASSERT_GT(rc, 0);
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc) == 0);

    //case2: read after seek
    offset = 123;
    hdfsSeek(fs, fin, offset);
    rc = hdfsRead(fs, fin, buf, to_read);
    ASSERT_GT(rc, 0);
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc) == 0);

    //case3: multi read
    offset = 456;
    hdfsSeek(fs, fin, offset);
    rc = hdfsRead(fs, fin, buf, to_read);
    ASSERT_GT(rc, 0);
    int rc2 = hdfsRead(fs, fin, buf + rc, to_read);
    ASSERT_GT(rc2, 0);
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc + rc2) == 0);
    //clean up
    int retval = hdfsCloseFile(fs, fin);
    ASSERT_TRUE(retval == 0);
    system("hadoop fs -rmr /TDEBasicRead");
    system("hadoop key delete keytde4basicread -f");
}

TEST(TestCInterfaceTDE, TestReadWithTDE_Advanced_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);

    //create a big file
    char cmd[128];
    const char *file_name = "tde_read_bigfile";
    int file_size = 150 * 1024 * 1024; //150M
    generate_file(file_name, file_size);

    //put file to TDE encryption zone
    system("hadoop fs -rmr /TDEAdvancedRead");
    system("hadoop key create keytde4advancedread");
    system("hadoop fs -mkdir /TDEAdvancedRead");
    ASSERT_EQ(0,
            hdfsCreateEncryptionZone(fs, "/TDEAdvancedRead",
                    "keytde4advancedread"));
    sprintf(cmd, "hdfs dfs -put `pwd`/%s /TDEAdvancedRead/", file_name);
    system(cmd);

    int offset = 0;
    int rc = 0;
    char *buf = (char *) malloc(8 * 1024 * 1024); //8M
    int to_read = 5;
    char file_path[128];
    sprintf(file_path, "/TDEAdvancedRead/%s", file_name);
    hdfsFile fin = hdfsOpenFile(fs, file_path, O_RDONLY, 0, 0, 0);
    //case4: skip block size(128M) read
    offset = 128 * 1024 * 1024 + 12345;
    hdfsSeek(fs, fin, offset);
    rc = hdfsRead(fs, fin, buf, to_read);

    ASSERT_GT(rc, 0);
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc) == 0);

    //case5: skip package size(64k) read
    offset = 64 * 1024 * 2 + 1234;
    hdfsSeek(fs, fin, offset);
    rc = hdfsRead(fs, fin, buf, to_read);
    ASSERT_GT(rc, 0);
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc) == 0);

    //case6: read block intervals
    offset = 128 * 1024 * 1024 - 123;
    to_read = 128;
    hdfsSeek(fs, fin, offset);
    rc = hdfsRead(fs, fin, buf, to_read);
    ASSERT_TRUE(rc == 123); //only in remote read
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc) == 0);

    //case7: read more bytes
    offset = 5678;
    to_read = 5 * 1024 * 1024 + 4567; //5M
    int off = 0;
    hdfsSeek(fs, fin, offset);
    while (to_read > 0) {
        rc = hdfsRead(fs, fin, buf + off, to_read);
        ASSERT_GT(rc, 0);
        std::cout << "loop read bytes:" << rc << std::endl;
        to_read -= rc;
        off += rc;
    }
    ASSERT_TRUE(diff_buf2filecontents(file_name, buf, offset, rc) == 0);

    //clean up
    int retval = hdfsCloseFile(fs, fin);
    ASSERT_TRUE(retval == 0);
    system("hadoop fs -rmr /TDEAdvancedRead");
    system("hadoop key delete keytde4advancedread -f");
    free(buf);
}

TEST(TestCInterfaceTDE, TestWriteReadWithTDE_Success) {
    hdfsFS fs = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    fs = hdfsBuilderConnect(bld);
    hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
    ASSERT_TRUE(fs != NULL);
    //Create encryption zone for test.
    system("hadoop fs -rmr /TDE");
    system("hadoop key create keytde");
    system("hadoop fs -mkdir /TDE");
    ASSERT_EQ(0, hdfsCreateEncryptionZone(fs, "/TDE", "keytde"));
    //Create tdefile under the encryption zone for TDE to write.
    const char *tdefile = "/TDE/testfile";
    //Write buffer to tdefile.
    const char *buffer = "test tde write and read function success";
    hdfsFile out = hdfsOpenFile(fs, tdefile, O_WRONLY | O_CREAT, 0, 0, 0);
    ASSERT_TRUE(out != NULL)<< hdfsGetLastError();
    EXPECT_EQ(strlen(buffer), hdfsWrite(fs, out, (const void *)buffer, strlen(buffer)))
            << hdfsGetLastError();
    hdfsCloseFile(fs, out);
    //Read buffer from tdefile with TDE read function.
    int offset = 0;
    int rc = 0;
    char buf[1024];
    hdfsFile fin = hdfsOpenFile(fs, tdefile, O_RDONLY, 0, 0, 0);
    rc = hdfsRead(fs, fin, buf, strlen(buffer));
    buf[strlen(buffer)] = '\0';
    ASSERT_GT(rc, 0);
    //Check the buffer is eaqual to the data reading from tdefile.
    ASSERT_STREQ(buffer, buf);
    int retval = hdfsCloseFile(fs, fin);
    ASSERT_TRUE(retval == 0);
    system("hadoop fs -rmr /TDE");
    system("hadoop key delete keytde -f");
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
}

