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
#ifndef _HDFS_LIBHDFS3_CLIENT_HTTPCLIENT_H_
#define _HDFS_LIBHDFS3_CLIENT_HTTPCLIENT_H_

#include <string>
#include <vector>
#include <curl/curl.h>
#include "Exception.h"
#include "ExceptionInternal.h"

typedef enum httpMethod {
    HTTP_GET = 0,
    HTTP_POST = 1,
    HTTP_DELETE = 2,
    HTTP_PUT = 3
} httpMethod;

namespace Hdfs {

class HttpClient {
public:
    HttpClient();

    /**
     * Construct a HttpClient instance.
     * @param url a url which is the address to send the request to the corresponding http server.
     */
    HttpClient(const std::string &url);

    /**
     * Destroy a HttpClient instance.
     */
    virtual ~HttpClient();

    /**
     * Set url for http client.
     */
    void setURL(const std::string &url);

    /**
     * Set headers for http client.
     */
    void setHeaders(const std::vector<std::string> &headers);

    /**
     * Set body for http client.
     */
    void setBody(const std::string &body);

    /**
     * Set retry times for http request which can be configured in config file.
     */
    void setRequestRetryTimes(int requst_retry_times);

    /**
     * Set request timeout which can be configured in config file.
     */
    void setRequestTimeout(int64_t curl_timeout);

    /**
     * Set expected response code.
     */
    void setExpectedResponseCode(int64_t response_code_ok);

    /**
     * Init curl handler and set options for curl.
     */
    void init();

    /**
     * Do clean up for curl.
     */
    void destroy();

    /**
     * Http POST method.
     */
    virtual std::string post();

    /**
     * Http DELETE method.
     */
    virtual std::string del();

    /**
     * Http PUT method.
     */
    virtual std::string put();

    /**
     * Http GET method.
     */
    virtual std::string get();

    /**
     * URL encodes the given string.
     */
    std::string escape(const std::string &data);

    /**
     * Receive error string from curl.
     */
    std::string errorString();

private:

    /**
     * Http common method to get response info by sending request to http server.
     * @param method : define different http methods.
     * @return return response info.
     */
    std::string httpCommon(httpMethod method);

    /**
     * Curl call back function to receive the reponse messages.
     * @return return the size of reponse messages.
     */
    static size_t CurlWriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp);

    static bool initialized;
    CURLcode res;
    std::string url;
    std::vector<std::string> headers;
    std::string body;
    int64_t response_code_ok;
    int request_retry_times;
    int64_t curl_timeout;
    CURL *curl;
    struct curl_slist *list;
    std::string response;
    char errbuf[CURL_ERROR_SIZE] = { 0 };
};

}
#endif
