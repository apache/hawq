/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
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

class AlreadyBeingCreatedException: public YarnException {
public:
    AlreadyBeingCreatedException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~AlreadyBeingCreatedException() throw () {
    }

public:
    static const char * ReflexName;
};

class ChecksumException: public YarnException {
public:
    ChecksumException(const std::string & arg, const char * file, int line,
                      const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~ChecksumException() throw () {
    }
};

class DSQuotaExceededException: public YarnException {
public:
    DSQuotaExceededException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~DSQuotaExceededException() throw () {
    }

public:
    static const char * ReflexName;
};

class FileAlreadyExistsException: public YarnException {
public:
    FileAlreadyExistsException(const std::string & arg, const char * file,
                               int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~FileAlreadyExistsException() throw () {
    }

public:
    static const char * ReflexName;
};

class FileNotFoundException: public YarnException {
public:
    FileNotFoundException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~FileNotFoundException() throw () {
    }

public:
    static const char * ReflexName;
};

class YarnBadBoolFoumat: public YarnException {
public:
    YarnBadBoolFoumat(const std::string & arg, const char * file, int line,
                      const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnBadBoolFoumat() throw () {
    }
};

class YarnBadConfigFoumat: public YarnException {
public:
    YarnBadConfigFoumat(const std::string & arg, const char * file, int line,
                        const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnBadConfigFoumat() throw () {
    }
};

class YarnBadNumFoumat: public YarnException {
public:
    YarnBadNumFoumat(const std::string & arg, const char * file, int line,
                     const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnBadNumFoumat() throw () {
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

class YarnFileSystemClosed: public YarnException {
public:
    YarnFileSystemClosed(const std::string & arg, const char * file, int line,
                         const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnFileSystemClosed() throw () {
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

class YarnInvalidBlockToken: public YarnException {
public:
    YarnInvalidBlockToken(const std::string & arg, const char * file, int line,
                          const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~YarnInvalidBlockToken() throw () {
    }

public:
    static const char * ReflexName;
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

class InvalidPath: public YarnException {
public:
    InvalidPath(const std::string & arg, const char * file, int line,
                const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~InvalidPath() throw () {
    }
};

class NotReplicatedYetException: public YarnException {
public:
    NotReplicatedYetException(const std::string & arg, const char * file,
                              int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~NotReplicatedYetException() throw () {
    }

public:
    static const char * ReflexName;
};

class NSQuotaExceededException: public YarnException {
public:
    NSQuotaExceededException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~NSQuotaExceededException() throw () {
    }

public:
    static const char * ReflexName;
};

class ParentNotDirectoryException: public YarnException {
public:
    ParentNotDirectoryException(const std::string & arg, const char * file,
                                int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~ParentNotDirectoryException() throw () {
    }

public:
    static const char * ReflexName;
};

class ReplicaNotFoundException: public YarnException {
public:
    ReplicaNotFoundException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        YarnException(arg, file, line, stack) {
    }

    ~ReplicaNotFoundException() throw () {
    }

public:
    static const char * ReflexName;
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


}

#endif /* _YARN_LIBYARN_COMMON_EXCEPTION_H_ */
