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

#ifndef _YARN_LIBYARN_COMMON_EXCEPTION_H_
#define _YARN_LIBYARN_COMMON_EXCEPTION_H_

#include <stdexcept>
#include <string>

namespace Yarn {

class YarnException: public std::runtime_error {
public:
    YarnException(const std::string & arg, const char * file, int line,
                  const char * stack);

    ~YarnException() throw () {
    }

    virtual const char * msg() const {
        return detail.c_str();
    }

protected:
    std::string detail;
};

class YarnIOException: public YarnException {
public:
    YarnIOException(const std::string & arg, const char * file, int line,
                    const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnIOException() throw () {
    }

public:
    static const char * ReflexName;
};

class YarnResourceManagerClosed: public YarnException {
public:
	YarnResourceManagerClosed(const std::string & arg, const char * file, int line,
                         const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnResourceManagerClosed() throw () {
    }
};

class YarnNetworkException: public YarnIOException {
public:
    YarnNetworkException(const std::string & arg, const char * file, int line,
                         const char * stack) :
        YarnIOException(arg, file, line, stack) {
    }

    ~YarnNetworkException() throw () {
    }
};

class YarnNetworkConnectException: public YarnNetworkException {
public:
    YarnNetworkConnectException(const std::string & arg, const char * file, int line,
                                const char * stack) :
        YarnNetworkException(arg, file, line, stack) {
    }

    ~YarnNetworkConnectException() throw () {
    }
};

class AccessControlException: public YarnException {
public:
    AccessControlException(const std::string & arg, const char * file, int line,
                           const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~AccessControlException() throw () {
    }

public:
    static const char * ReflexName;
};

class YarnBadBoolFormat: public YarnException {
public:
    YarnBadBoolFormat(const std::string & arg, const char * file, int line,
                      const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnBadBoolFormat() throw () {
    }
};

class YarnBadConfigFormat: public YarnException {
public:
    YarnBadConfigFormat(const std::string & arg, const char * file, int line,
                        const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnBadConfigFormat() throw () {
    }
};

class YarnBadNumFormat: public YarnException {
public:
    YarnBadNumFormat(const std::string & arg, const char * file, int line,
                     const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnBadNumFormat() throw () {
    }
};

class YarnCanceled: public YarnException {
public:
    YarnCanceled(const std::string & arg, const char * file, int line,
                 const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnCanceled() throw () {
    }
};

class YarnConfigInvalid: public YarnException {
public:
    YarnConfigInvalid(const std::string & arg, const char * file, int line,
                      const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnConfigInvalid() throw () {
    }
};

class YarnConfigNotFound: public YarnException {
public:
    YarnConfigNotFound(const std::string & arg, const char * file, int line,
                       const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnConfigNotFound() throw () {
    }
};

class YarnEndOfStream: public YarnIOException {
public:
    YarnEndOfStream(const std::string & arg, const char * file, int line,
                    const char * stack) :
        YarnIOException(arg, file, line, stack) {
    }

    ~YarnEndOfStream() throw () {
    }
};

/**
 * This will wrap YarnNetworkConnectionException and YarnTimeoutException.
 * This exception will be caught and attempt will be performed to recover in HA case.
 */
class YarnFailoverException: public YarnException {
public:
    YarnFailoverException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnFailoverException() throw () {
    }
};

/**
 * Fatal error during the rpc call. It may wrap other exceptions.
 */
class YarnRpcException: public YarnIOException {
public:
    YarnRpcException(const std::string & arg, const char * file, int line,
                     const char * stack) :
        YarnIOException(arg, file, line, stack) {
    }

    ~YarnRpcException() throw () {
    }
};

/**
 * Server throw an error during the rpc call.
 * It should be used internally and parsed for details.
 */
class YarnRpcServerException: public YarnIOException {
public:
    YarnRpcServerException(const std::string & arg, const char * file, int line,
                           const char * stack) :
        YarnIOException(arg, file, line, stack) {
    }

    ~YarnRpcServerException() throw () {
    }

    const std::string & getErrClass() const {
        return errClass;
    }

    void setErrClass(const std::string & errClass) {
        this->errClass = errClass;
    }

    const std::string & getErrMsg() const {
        return errMsg;
    }

    void setErrMsg(const std::string & errMsg) {
        this->errMsg = errMsg;
    }

private:
    std::string errClass;
    std::string errMsg;
};

class YarnTimeoutException: public YarnException {
public:
    YarnTimeoutException(const std::string & arg, const char * file, int line,
                         const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnTimeoutException() throw () {
    }
};

class InvalidParameter: public YarnException {
public:
    InvalidParameter(const std::string & arg, const char * file, int line,
                     const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~InvalidParameter() throw () {
    }
};

class SafeModeException: public YarnException {
public:
    SafeModeException(const std::string & arg, const char * file, int line,
                      const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~SafeModeException() throw () {
    }

public:
    static const char * ReflexName;
};

class UnresolvedLinkException: public YarnException {
public:
    UnresolvedLinkException(const std::string & arg, const char * file,
                            int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~UnresolvedLinkException() throw () {
    }

public:
    static const char * ReflexName;
};

class UnsupportedOperationException: public YarnException {
public:
    UnsupportedOperationException(const std::string & arg, const char * file,
                                  int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~UnsupportedOperationException() throw () {
    }

public:
    static const char * ReflexName;
};

class SaslException: public YarnException {
public:
    SaslException(const std::string & arg, const char * file, int line,
                  const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~SaslException() throw () {
    }

public:
    static const char * ReflexName;
};

class ResourceManagerStandbyException: public YarnException {
public:
    ResourceManagerStandbyException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~ResourceManagerStandbyException() throw () {
    }

public:
    static const char * ReflexName;
};

class ApplicationMasterNotRegisteredException: public YarnException {
public:
    ApplicationMasterNotRegisteredException(const std::string & arg, const char * file,
                                            int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~ApplicationMasterNotRegisteredException() throw () {
    }

public:
    static const char * ReflexName;
};
}
#endif /* _YARN_LIBYARN_COMMON_EXCEPTION_H_ */
