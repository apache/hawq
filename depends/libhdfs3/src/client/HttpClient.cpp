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

#include "HttpClient.h"
#include "Logger.h"

using namespace Hdfs::Internal;

namespace Hdfs {

#define CURL_SETOPT(handle, option, optarg, fmt, ...) \
    res = curl_easy_setopt(handle, option, optarg); \
    if (res != CURLE_OK) { \
        THROW(HdfsIOException, fmt, ##__VA_ARGS__); \
    }

#define CURL_SETOPT_ERROR1(handle, option, optarg, fmt) \
    CURL_SETOPT(handle, option, optarg, fmt, curl_easy_strerror(res));

#define CURL_SETOPT_ERROR2(handle, option, optarg, fmt) \
    CURL_SETOPT(handle, option, optarg, fmt, curl_easy_strerror(res), \
        errorString().c_str())

#define CURL_PERFORM(handle, fmt) \
    res = curl_easy_perform(handle); \
    if (res != CURLE_OK) { \
        THROW(HdfsIOException, fmt, curl_easy_strerror(res), errorString().c_str()); \
    }

#define CURL_GETOPT_ERROR2(handle, option, optarg, fmt) \
    res = curl_easy_getinfo(handle, option, optarg); \
    if (res != CURLE_OK) { \
        THROW(HdfsIOException, fmt, curl_easy_strerror(res), errorString().c_str()); \
    }

#define CURL_GET_RESPONSE(handle, code, fmt) \
    CURL_GETOPT_ERROR2(handle, CURLINFO_RESPONSE_CODE, code, fmt);

HttpClient::HttpClient() : curl(NULL), list(NULL) {
}

/**
 * Construct a HttpClient instance.
 * @param url a url which is the address to send the request to the corresponding http server.
 */
HttpClient::HttpClient(const std::string &url) {
    curl = NULL;
    list = NULL;
    this->url = url;
}

/**
 * Destroy a HttpClient instance.
 */
HttpClient::~HttpClient()
{
    destroy();
}

/**
 * Receive error string from curl.
 */
std::string HttpClient::errorString() {
    if (strlen(errbuf) == 0) {
        return "";
    }
    return errbuf;
}

/**
 * Curl call back function to receive the reponse messages.
 * @return return the size of reponse messages. 
 */
size_t HttpClient::CurlWriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    if (userp == NULL || contents == NULL) {
        return 0;
    }
    ((std::string *) userp)->append((const char *) contents, realsize);
    LOG(DEBUG3, "HttpClient : Http response is : %s", ((std::string * )userp)->c_str());
    return realsize;
}

/**
 * Init curl handler and set curl options.
 */
void HttpClient::init() {
    if (!initialized) {
        initialized = true;
        if (curl_global_init (CURL_GLOBAL_ALL)) {
            THROW(HdfsIOException, "Cannot initialize curl client for KMS");
        }
    }

	curl = curl_easy_init();
	if (!curl) {
		THROW(HdfsIOException, "Cannot initialize curl handle for KMS");
	}
	
    CURL_SETOPT_ERROR1(curl, CURLOPT_ERRORBUFFER, errbuf,
        "Cannot initialize curl error buffer for KMS: %s");

    errbuf[0] = 0;

    CURL_SETOPT_ERROR2(curl, CURLOPT_NOPROGRESS, 1,
        "Cannot initialize no progress in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_VERBOSE, 0,
        "Cannot initialize no verbose in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_COOKIEFILE, "",
        "Cannot initialize cookie behavior in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_HTTPHEADER, list,
        "Cannot initialize headers in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_WRITEFUNCTION, HttpClient::CurlWriteMemoryCallback,
        "Cannot initialize body reader in HttpClient: %s: %s");

    CURL_SETOPT_ERROR2(curl, CURLOPT_WRITEDATA, (void *)&response,
        "Cannot initialize body reader data in HttpClient: %s: %s");


    /* Some servers don't like requests that are made without a user-agent
     * field, so we provide one
     */
    CURL_SETOPT_ERROR2(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0",
        "Cannot initialize user agent in HttpClient: %s: %s");
    list = NULL;
}

/**
 * Do clean up for curl.
 */
void HttpClient::destroy() {
    if (curl) {
        curl_easy_cleanup(curl);
        curl = NULL;
    }
    if (list) {
        curl_slist_free_all(list);
        list = NULL;
    }
    initialized = false;
}

/**
 * Set url for http client.
 */
void HttpClient::setURL(const std::string &url) {
    this->url = url;
}

/**
 * Set retry times for http request which can be configured in config file.
 */
void HttpClient::setRequestRetryTimes(int request_retry_times) {
    if (request_retry_times < 0) {
        THROW(InvalidParameter, "HttpClient : Invalid value for request_retry_times.");
    }
    this->request_retry_times = request_retry_times;
}

/**
 * Set request timeout which can be configured in config file.
 */
void HttpClient::setRequestTimeout(int64_t curl_timeout) {
    if (curl_timeout < 0) {
        THROW(InvalidParameter, "HttpClient : Invalid value for curl_timeout.");
    }
    this->curl_timeout = curl_timeout;
}

/**
 * Set headers for http client.
 */
void HttpClient::setHeaders(const std::vector<std::string> &headers) {
    if (!headers.empty()) {
        this->headers = headers;
        for (std::string header : headers) {
            list = curl_slist_append(list, header.c_str());
            if (!list) {
                THROW(HdfsIOException, "Cannot add header in HttpClient.");
            }
        }
    } else {
        LOG(DEBUG3, "HttpClient : Header is empty.");
    }
}


/**
 * Set body for http client.
 */
void HttpClient::setBody(const std::string &body) {
    this->body = body;
}

/**
 * Set expected response code.
 */
void HttpClient::setExpectedResponseCode(int64_t response_code_ok) {
    this->response_code_ok = response_code_ok;
}

/**
 * Http common method to get response info by sending request to http server.
 * @param method : define different http methods.
 * @return return response info.
 */
std::string HttpClient::httpCommon(httpMethod method) {
    /* Set headers and url. */
    if (list != NULL) {
        CURL_SETOPT_ERROR2(curl, CURLOPT_HTTPHEADER, list,
                "Cannot initialize headers in HttpClient: %s: %s");
    } else {
        LOG(DEBUG3, "HttpClient : Http Header is NULL");
    }

    if (curl != NULL) {
        CURL_SETOPT_ERROR2(curl, CURLOPT_URL, url.c_str(),
                "Cannot initialize url in HttpClient: %s: %s");
    } else {
        LOG(LOG_ERROR, "HttpClient : Http URL is NULL");
    }

    /* Set body based on different http method. */
    switch (method) {
        case HTTP_GET:
        {
            break;
        }
        case HTTP_POST:
        {
            CURL_SETOPT_ERROR2(curl, CURLOPT_COPYPOSTFIELDS, body.c_str(),
                    "Cannot initialize post data in HttpClient: %s: %s");
            break;
        }
        case HTTP_DELETE:
        {
            CURL_SETOPT_ERROR2(curl, CURLOPT_CUSTOMREQUEST, "DELETE",
                    "Cannot initialize set customer request in HttpClient: %s: %s");
            break;
        }
        case HTTP_PUT:
        {
            CURL_SETOPT_ERROR2(curl, CURLOPT_CUSTOMREQUEST, "PUT",
                    "Cannot initialize set customer request in HttpClient: %s: %s");
            CURL_SETOPT_ERROR2(curl, CURLOPT_COPYPOSTFIELDS, body.c_str(),
                    "Cannot initialize post data in HttpClient: %s: %s");
            break;
        }
        default:
        {
            LOG(LOG_ERROR, "HttpClient : unknown method: %d", method);
        }
    }

    /* Do several http request try according to request_retry_times
     * until got the right response code.
     */
    int64_t response_code = -1;

    while (request_retry_times >= 0 && response_code != response_code_ok) {
        request_retry_times -= 1;
        response = "";
        CURL_SETOPT_ERROR2(curl, CURLOPT_TIMEOUT, curl_timeout,
                "Send request to http server timeout: %s: %s");
        CURL_PERFORM(curl, "Could not send request in HttpClient: %s %s");
        CURL_GET_RESPONSE(curl, &response_code,
                "Cannot get response code in HttpClient: %s: %s");
    }
    LOG(DEBUG3, "HttpClient : The http method is %d. The http url is %s. The http response is %s.",
            method, url.c_str(), response.c_str());
    return response;
}

/**
 * Http GET method.
 */
std::string HttpClient::get() {
    return httpCommon(HTTP_GET);
}

/**
 * Http POST method.
 */
std::string HttpClient::post() {
    return httpCommon(HTTP_POST);
}

/**
 * Http DELETE method.
 */
std::string HttpClient::del() {
    return httpCommon(HTTP_DELETE);
}

/**
 * Http PUT method.
 */
std::string HttpClient::put() {
    return httpCommon(HTTP_PUT);
}


/**
 *  URL encodes the given string. 
 */
std::string HttpClient::escape(const std::string &data) {
    if (curl) {
        char *output = curl_easy_escape(curl, data.c_str(), data.length());
        if (output) {
            std::string out(output);
            return out;
        } else {
            THROW(HdfsIOException, "HttpClient : Curl escape failed.");
        }
    } else {
        LOG(WARNING, "HttpClient : Curl in escape method is NULL");
    }
    std::string empty;
    return empty;
}
}

/* Curl global init only can be done once. */
bool Hdfs::HttpClient::initialized = false;

