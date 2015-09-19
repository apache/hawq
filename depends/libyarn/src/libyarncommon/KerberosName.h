/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_CLIENT_KERBEROSNAME_H_
#define _HDFS_LIBHDFS3_CLIENT_KERBEROSNAME_H_

#include <string>
#include <sstream>

#include "Hash.h"

namespace Yarn {
namespace Internal {

class KerberosName {
public:
    KerberosName();
    KerberosName(const std::string & principal);

    std::string getPrincipal() const {
        std::stringstream ss;
        ss << name;

        if (!host.empty()) {
            ss << "/" << host;
        }

        if (!realm.empty()) {
            ss << '@' << realm;
        }

        return ss.str();
    }

    const std::string & getHost() const {
        return host;
    }

    void setHost(const std::string & host) {
        this->host = host;
    }

    const std::string & getName() const {
        return name;
    }

    void setName(const std::string & name) {
        this->name = name;
    }

    const std::string & getRealm() const {
        return realm;
    }

    void setRealm(const std::string & realm) {
        this->realm = realm;
    }

    size_t hash_value() const;

    bool operator ==(const KerberosName & other) const {
        return name == other.name && host == other.host && realm == other.realm;
    }

private:
    void parse(const std::string & principal);

private:
    std::string name;
    std::string host;
    std::string realm;
};

}
}

YARN_HASH_DEFINE(::Yarn::Internal::KerberosName);

#endif /* _HDFS_LIBHDFS3_CLIENT_KERBEROSNAME_H_ */
