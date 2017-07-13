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
#ifndef _HDFS_LIBHDFS3_CLIENT_KMSCLIENTPROVIDER_H_
#define _HDFS_LIBHDFS3_CLIENT_KMSCLIENTPROVIDER_H_

#include <string>
#include <gsasl.h>

#include "openssl/conf.h"
#include "openssl/evp.h"
#include "openssl/err.h"
#include "FileEncryptionInfo.h"
#include "HttpClient.h"
#include <vector>
#include "common/SessionConfig.h"
#include "rpc/RpcAuth.h"
#include "common/Memory.h"
#include <boost/property_tree/ptree.hpp>

using boost::property_tree::ptree;
using namespace Hdfs::Internal;


namespace Hdfs {

class KmsClientProvider {
public:

    /**
     * Construct a KmsClientProvider instance.
     * @param auth RpcAuth to get the auth method and user info.
     * @param conf a SessionConfig to get the configuration.
     */
    KmsClientProvider(shared_ptr<RpcAuth> auth, shared_ptr<SessionConfig> conf);

    /**
     * Destroy a KmsClientProvider instance.
     */
    virtual ~KmsClientProvider() {
    }

    /**
     * Set HttpClient object.
     */
    void setHttpClient(shared_ptr<HttpClient> hc);

    /**
     * Create an encryption key from kms.
     * @param keyName the name of this key.
     * @param cipher the ciphertext of this key. e.g. "AES/CTR/NoPadding" .
     * @param length the length of this key.
     * @param material will be encode to base64.
     * @param description key's info.
     */
    virtual void createKey(const std::string &keyName, const std::string &cipher, const int length, const std::string &material, const std::string &description);

    /**
     * Get key metadata based on encrypted file's key name.
     * @param encryptionInfo the encryption info of file.
     * @return return response info about key metadata from kms server.
     */
    virtual ptree getKeyMetadata(const FileEncryptionInfo &encryptionInfo);

    /**
     * Delete an encryption key from kms.
     * @param encryptionInfo the encryption info of file.
     */
    virtual void deleteKey(const FileEncryptionInfo &encryptionInfo);

    /**
     * Decrypt an encrypted key from kms.
     * @param encryptionInfo the encryption info of file.
     * @return return decrypted key.
     */
    virtual ptree decryptEncryptedKey(const FileEncryptionInfo &encryptionInfo);

    /**
     * Encode string to base64.
     */
    static std::string base64Encode(const std::string &data);

    /**
     * Decode base64 to string.
     */
    static std::string base64Decode(const std::string &data);

private:

    /**
     * Convert ptree format to json format.
     */
    static std::string toJson(const ptree &data);

    /**
     * Convert json format to ptree format.
     */
    static ptree fromJson(const std::string &data);

    /**
     * Parse kms url from configure file.
     */
    std::string parseKmsUrl();

    /**
     * Build kms url based on urlSuffix and different auth method.
     */
    std::string buildKmsUrl(const std::string &url, const std::string &urlSuffix);
    /**
     * Set common headers for kms API.
     */
    void setCommonHeaders(std::vector<std::string>& headers);

    shared_ptr<HttpClient> hc;
    std::string url;

    shared_ptr<RpcAuth> auth;
    AuthMethod method;
    shared_ptr<SessionConfig> conf;

};

}
#endif
