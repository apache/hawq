/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "UserInfo.h"

#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

#include <vector>

#include "Exception.h"
#include "ExceptionInternal.h"

namespace Yarn {
namespace Internal {

UserInfo UserInfo::LocalUser() {
    UserInfo retval;
    uid_t uid, euid;
    int bufsize;
    struct passwd pwd, epwd, *result = NULL;
    euid = geteuid();
    uid = getuid();

    if ((bufsize = sysconf(_SC_GETPW_R_SIZE_MAX)) == -1) {
        THROW(InvalidParameter,
              "Invalid input: \"sysconf\" function failed to get the configure with key \"_SC_GETPW_R_SIZE_MAX\".");
    }

    std::vector<char> buffer(bufsize);

    if (getpwuid_r(euid, &epwd, &buffer[0], bufsize, &result) != 0 || !result) {
        THROW(InvalidParameter,
              "Invalid input: effective user name cannot be found with UID %u.",
              euid);
    }

    retval.setEffectiveUser(epwd.pw_name);

    if (getpwuid_r(uid, &pwd, &buffer[0], bufsize, &result) != 0 || !result) {
        THROW(InvalidParameter,
              "Invalid input: real user name cannot be found with UID %u.",
              uid);
    }

    retval.setRealUser(pwd.pw_name);
    return retval;
}

size_t UserInfo::hash_value() const {
    size_t values[] = { StringHasher(realUser), effectiveUser.hash_value() };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
