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

#include "KmsClientProvider.h"
#include "Logger.h"
#include <gsasl.h>
#include <map>
#include <boost/property_tree/json_parser.hpp>
using namespace Hdfs::Internal;
using boost::property_tree::read_json;
using boost::property_tree::write_json;

namespace Hdfs {

/**
 * Convert ptree format to json format
 */
std::string KmsClientProvider::toJson(const ptree &data) {
    std::ostringstream buf;
    try {
        write_json(buf, data, false);
        std::string json = buf.str();
        return json;
    } catch (...) {
        THROW(HdfsIOException, "KmsClientProvider : Write json failed.");
    }
}

/**
 * Convert json format to ptree format
 */
ptree KmsClientProvider::fromJson(const std::string &data) {
    ptree pt2;
    try {
        std::istringstream is(data);
        read_json(is, pt2);
        return pt2;
    } catch (...) {
        THROW(HdfsIOException, "KmsClientProvider : Read json failed.");
    }
}

/**
 * Encode string to base64. 
 */
std::string KmsClientProvider::base64Encode(const std::string &data) {
    char * buffer = NULL;
    size_t len = 0;
    int rc = 0;
    std::string result;

    LOG(DEBUG3, "KmsClientProvider : Encode data is %s", data.c_str());

    if (GSASL_OK != (rc = gsasl_base64_to(data.data(), data.size(), &buffer, &len))) {
        assert(GSASL_MALLOC_ERROR == rc);
        throw std::bad_alloc();
    }

    if (buffer) {
        result.assign(buffer, len);
        free(buffer);
    }

    if (!buffer || result.length() != len) {
        THROW(HdfsIOException,
                "KmsClientProvider: Failed to encode string to base64");
    }

    return result;
}

/**
 * Decode base64 to string.
 */
std::string KmsClientProvider::base64Decode(const std::string &data) {
    char * buffer = NULL;
    size_t len = 0;
    int rc = 0;
    std::string result;

    if (GSASL_OK != (rc = gsasl_base64_from(data.data(), data.size(), &buffer, &len))) {
        assert(GSASL_MALLOC_ERROR == rc);
        throw std::bad_alloc();
    }

    if (buffer) {
        result.assign(buffer, len);
        free(buffer);
    }

    if (!buffer || result.length() != len) {
        THROW(HdfsIOException,
                "KmsClientProvider: Failed to decode base64 to string");
    }

    return result;
}

/**
 * Construct a KmsClientProvider instance.
 * @param auth RpcAuth to get the auth method and user info.
 * @param conf a SessionConfig to get the configuration.
 */
KmsClientProvider::KmsClientProvider(shared_ptr<RpcAuth> rpcAuth, shared_ptr<SessionConfig> config) : auth(rpcAuth), conf(config)
{
    hc.reset(new HttpClient());
    method = RpcAuth::ParseMethod(conf->getKmsMethod());
}

/**
 * Set HttpClient object.
 */
void KmsClientProvider::setHttpClient(shared_ptr<HttpClient> hc)
{
    this->hc = hc;
}

/**
 * Parse kms url from configure file.
 */
std::string KmsClientProvider::parseKmsUrl() 
{
    std::string start = "kms://";
    std::string http = "http@";
    std::string https = "https@";
    std::string urlParse = conf->getKmsUrl();
    LOG(DEBUG3, "KmsClientProvider : Get kms url from conf : %s.",
            urlParse.c_str());
    if (urlParse.compare(0, start.length(), start) == 0) {
        start = urlParse.substr(start.length());
        if (start.compare(0, http.length(), http) == 0) {
            return "http://" + start.substr(http.length());
        } else if (start.compare(0, https.length(), https) == 0) {
            return "https://" + start.substr(https.length());
        } else
            THROW(HdfsIOException, "Bad KMS provider URL: %s", urlParse.c_str());
    } else
        THROW(HdfsIOException, "Bad KMS provider URL: %s", urlParse.c_str());

}

/**
 * Build kms url based on urlSuffix and different auth method. 
 */
std::string KmsClientProvider::buildKmsUrl(const std::string &url, const std::string &urlSuffix)
{
    std::string baseUrl = url;
    baseUrl = url + "/v1/" + urlSuffix;
    std::size_t found = urlSuffix.find('?');

    if (method == AuthMethod::KERBEROS) {
        // todo
        THROW(InvalidParameter, "KmsClientProvider : Not support kerberos yet.");
    } else if (method == AuthMethod::SIMPLE) {
        std::string user = auth->getUser().getRealUser();
        LOG(DEBUG3,
                "KmsClientProvider : Kms urlSuffix is : %s. Auth real user is : %s.",
                urlSuffix.c_str(), user.c_str());
        if (user.length() == 0)
            user = auth->getUser().getKrbName();
        if (found != std::string::npos)
            return baseUrl + "&user.name=" + user;
        else
            return baseUrl + "?user.name=" + user;
    } else {
        return baseUrl;
    }
}

/**
 * Set common headers for kms API.
 */
void KmsClientProvider::setCommonHeaders(std::vector<std::string>& headers)
{
    headers.push_back("Content-Type: application/json");
    headers.push_back("Accept: *");
}


/**
 * Create an encryption key from kms.
 * @param keyName the name of this key.
 * @param cipher the ciphertext of this key. e.g. "AES/CTR/NoPadding" .
 * @param length the length of this key.
 * @param material will be encode to base64.
 * @param description key's info.
 */
void KmsClientProvider::createKey(const std::string &keyName, const std::string &cipher, const int length, const std::string &material, const std::string &description)
{
    hc->init();
    /* Prepare url for HttpClient.*/
    url = parseKmsUrl();
    std::string urlSuffix = "keys";
    url = buildKmsUrl(url, urlSuffix);
    /* Prepare headers for HttpClient.*/
    std::vector<std::string> headers;
    setCommonHeaders(headers);
    /* Prepare body for HttpClient. */
    ptree map;
    map.put("name", keyName);
    map.put("cipher", cipher);
    map.put("description", description);
    std::string body = toJson(map);
    /* Set options for HttpClient to get response. */
    hc->setURL(url);
    hc->setHeaders(headers);
    hc->setBody(body);
    hc->setRequestRetryTimes(conf->getHttpRequestRetryTimes());
    hc->setRequestTimeout(conf->getCurlTimeOut());
    hc->setExpectedResponseCode(201);
    std::string response = hc->post();

    LOG(DEBUG3,
            "KmsClientProvider::createKey : The key name, key cipher, key length, key material, description are : %s, %s, %d, %s, %s. The kms url is : %s . The kms body is : %s. The response of kms server is : %s .",
            keyName.c_str(), cipher.c_str(), length, material.c_str(),
            description.c_str(), url.c_str(), body.c_str(), response.c_str());

} 

/**
 * Get key metadata based on encrypted file's key name. 
 * @param encryptionInfo the encryption info of file.
 * @return return response info about key metadata from kms server.
 */
ptree KmsClientProvider::getKeyMetadata(const FileEncryptionInfo &encryptionInfo)
{
    hc->init();
    url = parseKmsUrl();
    std::string urlSuffix = "key/" + hc->escape(encryptionInfo.getKeyName()) + "/_metadata";
    url = buildKmsUrl(url, urlSuffix);

    hc->setURL(url);
    hc->setExpectedResponseCode(200);
    hc->setRequestRetryTimes(conf->getHttpRequestRetryTimes());
    hc->setRequestTimeout(conf->getCurlTimeOut());
    std::string response = hc->get();

    LOG(DEBUG3,
            "KmsClientProvider::getKeyMetadata : The kms url is : %s. The response of kms server is : %s .",
            url.c_str(), response.c_str());

    return fromJson(response);

}

/**
 * Delete an encryption key from kms. 
 * @param encryptionInfo the encryption info of file.
 */
void KmsClientProvider::deleteKey(const FileEncryptionInfo &encryptionInfo)
{
    hc->init();
    url = parseKmsUrl();
    std::string urlSuffix = "key/" + hc->escape(encryptionInfo.getKeyName());
    url = buildKmsUrl(url, urlSuffix);

    hc->setURL(url);
    hc->setExpectedResponseCode(200);
    hc->setRequestRetryTimes(conf->getHttpRequestRetryTimes());
    hc->setRequestTimeout(conf->getCurlTimeOut());
    std::string response = hc->del();

    LOG(DEBUG3,
            "KmsClientProvider::deleteKey : The kms url is : %s. The response of kms server is : %s .",
            url.c_str(), response.c_str());
}

/**
 * Decrypt an encrypted key from kms.
 * @param encryptionInfo the encryption info of file.
 * @return return decrypted key.
 */
ptree KmsClientProvider::decryptEncryptedKey(const FileEncryptionInfo &encryptionInfo)
{
    hc->init();
    /* Prepare HttpClient url. */
    url = parseKmsUrl();
    std::string urlSuffix = "keyversion/" + hc->escape(encryptionInfo.getEzKeyVersionName()) + "/_eek?eek_op=decrypt";
    url = buildKmsUrl(url, urlSuffix);
    /* Prepare HttpClient headers. */
    std::vector<std::string> headers;
    setCommonHeaders(headers);
    /* Prepare HttpClient body with json format. */
    ptree map;
    map.put("name", encryptionInfo.getKeyName());
    map.put("iv", base64Encode(encryptionInfo.getIv()));
    map.put("material", base64Encode(encryptionInfo.getKey()));
    std::string body = toJson(map);

    /* Set options for HttpClient. */
    hc->setURL(url);
    hc->setHeaders(headers);
    hc->setBody(body);
    hc->setExpectedResponseCode(200);
    hc->setRequestRetryTimes(conf->getHttpRequestRetryTimes());
    hc->setRequestTimeout(conf->getCurlTimeOut());
    std::string response = hc->post();

    LOG(DEBUG3,
            "KmsClientProvider::decryptEncryptedKey : The kms url is : %s . The kms body is : %s. The response of kms server is : %s .",
            url.c_str(), body.c_str(), response.c_str());
    return fromJson(response);
}

}

