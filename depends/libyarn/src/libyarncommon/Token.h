/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_CLIENT_TOKEN_H_
#define _HDFS_LIBHDFS3_CLIENT_TOKEN_H_

#include <string>

namespace Yarn {

class Token {
public:
    const std::string & getIdentifier() const {
        return identifier;
    }

    void setIdentifier(const std::string & identifier) {
        this->identifier = identifier;
    }

    const std::string & getKind() const {
        return kind;
    }

    void setKind(const std::string & kind) {
        this->kind = kind;
    }

    const std::string & getPassword() const {
        return password;
    }

    void setPassword(const std::string & password) {
        this->password = password;
    }

    const std::string & getService() const {
        return service;
    }

    void setService(const std::string & service) {
        this->service = service;
    }

    bool operator ==(const Token & other) const {
        return identifier == other.identifier && password == other.password
               && kind == other.kind && service == other.service;
    }

    size_t hash_value() const;

private:
    std::string identifier;
    std::string password;
    std::string kind;
    std::string service;
};

}

#endif /* _HDFS_LIBHDFS3_CLIENT_TOKEN_H_ */
