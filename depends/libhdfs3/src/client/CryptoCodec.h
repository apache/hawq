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
#ifndef _HDFS_LIBHDFS3_CLIENT_CRYPTOCODEC_H_
#define _HDFS_LIBHDFS3_CLIENT_CRYPTOCODEC_H_

#include <string>

#include "openssl/conf.h"
#include "openssl/evp.h"
#include "openssl/err.h"
#include "FileEncryptionInfo.h"
#include "KmsClientProvider.h"

#define KEY_LENGTH_256 32
#define KEY_LENGTH_128 16

namespace Hdfs {

class CryptoCodec {
public:
    /**
     * Construct a CryptoCodec instance.
     * @param encryptionInfo the encryption info of file.
     * @param kcp a KmsClientProvider instance to get key from kms server.
     * @param bufSize crypto buffer size.
     */
    CryptoCodec(FileEncryptionInfo *encryptionInfo, shared_ptr<KmsClientProvider> kcp, int32_t bufSize);

    /**
     * Destroy a CryptoCodec instance.
     */
    virtual ~CryptoCodec();

    /**
     * Encode buffer.
     */
    virtual std::string encode(const char * buffer, int64_t size);

    /**
     * Decode buffer.
     */
    virtual std::string decode(const char * buffer, int64_t size);

private:

    /**
     * Common encode/decode buffer method.
     * @param buffer the buffer to be encode/decode.
     * @param size the size of buffer.
     * @param enc true is for encode, false is for decode.
     * @return return the encode/decode buffer.
     */
    std::string endecInternal(const char *buffer, int64_t size, bool enc);

    /**
     * Get decrypted key from kms.
     */
    std::string getDecryptedKeyFromKms();

    shared_ptr<KmsClientProvider> kcp;
    FileEncryptionInfo *encryptionInfo;
    EVP_CIPHER_CTX *encryptCtx;
    EVP_CIPHER_CTX *decryptCtx;
    const EVP_CIPHER *cipher;
    int32_t bufSize;
};

}
#endif
