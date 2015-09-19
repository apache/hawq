/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_CLIENT_USERINFO_H_
#define _HDFS_LIBHDFS3_CLIENT_USERINFO_H_

#include <map>
#include <string>

#include "Hash.h"
#include "KerberosName.h"
#include "Token.h"

#include "Logger.h"

namespace Yarn {
namespace Internal {

class UserInfo {
public:
    UserInfo() {
    }

    explicit UserInfo(const std::string & u) :
        effectiveUser(u) {
    }

    const std::string & getRealUser() const {
        return realUser;
    }

    void setRealUser(const std::string & user) {
        this->realUser = user;
    }

    const std::string & getEffectiveUser() const {
        return effectiveUser.getName();
    }

    void setEffectiveUser(const std::string & effectiveUser) {
        this->effectiveUser = KerberosName(effectiveUser);
    }

    std::string getPrincipal() const {
        return effectiveUser.getPrincipal();
    }

    bool operator ==(const UserInfo & other) const {
        return realUser == other.realUser
               && effectiveUser == other.effectiveUser;
    }

    void addToken(const Token & token) {
        tokens[std::make_pair(token.getKind(), token.getService())] = token;
    }

    const Token * selectToken(const std::string & kind, const std::string & service) const {
        std::map<std::pair<std::string, std::string>, Token>::const_iterator it;
        it = tokens.find(std::make_pair(kind, service));

        if (it == tokens.end()) {
            return NULL;
        }

        return &it->second;
    }

    size_t hash_value() const;

public:
    static UserInfo LocalUser();

private:
    KerberosName effectiveUser;
    std::map<std::pair<std::string, std::string>, Token> tokens;
    std::string realUser;
};

}
}

YARN_HASH_DEFINE(::Yarn::Internal::UserInfo);

#endif /* _HDFS_LIBHDFS3_CLIENT_USERINFO_H_ */
