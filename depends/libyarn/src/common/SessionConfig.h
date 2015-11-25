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

#ifndef _YARN_LIBYARN_COMMON_SESSIONCONFIG_H_
#define _YARN_LIBYARN_COMMON_SESSIONCONFIG_H_

#include "Exception.h"
#include "ExceptionInternal.h"
#include "Function.h"
#include "Logger.h"
#include "XmlConfig.h"

#include <cassert>
#include <vector>

namespace Yarn {
namespace Internal {

template<typename T>
struct ConfigDefault {
    T * variable; //variable this configure item should be bound to.
    const char * key; //configure key.
    T value; //default value.
    function<void(const char *, T const &)> check;   //the function to validate the value.
};

class SessionConfig {
public:

    SessionConfig(const Config & conf);

    /*
     * rpc configure
     */

    int32_t getRpcConnectTimeout() const {
        return rpcConnectTimeout;
    }

    int32_t getRpcMaxIdleTime() const {
        return rpcMaxIdleTime;
    }

    int32_t getRpcMaxRetryOnConnect() const {
        return rpcMaxRetryOnConnect;
    }

    int32_t getRpcPingTimeout() const {
        return rpcPingTimeout;
    }

    int32_t getRpcReadTimeout() const {
        return rpcReadTimeout;
    }

    bool isRpcTcpNoDelay() const {
        return rpcTcpNoDelay;
    }

    int32_t getRpcWriteTimeout() const {
        return rpcWriteTimeout;
    }

    void setRpcMaxHaRetry(int32_t rpcMaxHaRetry) {
        rpcMaxHARetry = rpcMaxHaRetry;
    }

    int32_t getRpcMaxHaRetry() const {
        return rpcMaxHARetry;
    }

    const std::string & getRpcAuthMethod() const {
        return rpcAuthMethod;
    }

    void setRpcAuthMethod(const std::string & rpcAuthMethod) {
        this->rpcAuthMethod = rpcAuthMethod;
    }

    const std::string & getKerberosCachePath() const {
        return kerberosCachePath;
    }

    void setKerberosCachePath(const std::string & kerberosCachePath) {
        this->kerberosCachePath = kerberosCachePath;
    }

    int32_t getRpcSocketLingerTimeout() const {
        return rpcSocketLingerTimeout;
    }

    void setRpcSocketLingerTimeout(int32_t rpcSocketLingerTimeout) {
        this->rpcSocketLingerTimeout = rpcSocketLingerTimeout;
    }

    LogSeverity getLogSeverity() const {
        for (size_t i = FATAL; i < sizeof(SeverityName) / sizeof(SeverityName[1]);
                ++i) {
            if (logSeverity == SeverityName[i]) {
                return static_cast<LogSeverity>(i);
            }
        }

        return DEFAULT_LOG_LEVEL;
    }

    void setLogSeverity(const std::string & logSeverityLevel) {
        this->logSeverity = logSeverityLevel;
    }

    int32_t getRpcTimeout() const {
        return rpcTimeout;
    }

    void setRpcTimeout(int32_t rpcTimeout) {
        this->rpcTimeout = rpcTimeout;
    }

public:
    /*
     * rpc configure
     */
    int32_t rpcMaxIdleTime;
    int32_t rpcPingTimeout;
    int32_t rpcConnectTimeout;
    int32_t rpcReadTimeout;
    int32_t rpcWriteTimeout;
    int32_t rpcMaxRetryOnConnect;
    int32_t rpcMaxHARetry;
    int32_t rpcSocketLingerTimeout;
    int32_t rpcTimeout;
    bool rpcTcpNoDelay;
    std::string rpcAuthMethod;

    std::string kerberosCachePath;
    std::string logSeverity;
};

}
}

#endif /* _YARN_LIBYARN_COMMON_SESSIONCONFIG_H_ */
