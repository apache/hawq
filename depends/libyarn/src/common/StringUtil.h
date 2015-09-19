/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_STRINGUTIL_H_
#define _HDFS_LIBHDFS3_COMMON_STRINGUTIL_H_

#include <string.h>
#include <string>
#include <vector>
#include <cctype>

namespace Yarn {
namespace Internal {

static inline std::vector<std::string> StringSplit(const std::string & str,
        const char * sep) {
    char * token, *lasts = NULL;
    std::string s = str;
    std::vector<std::string> retval;
    token = strtok_r(&s[0], sep, &lasts);

    while (token) {
        retval.push_back(token);
        token = strtok_r(NULL, sep, &lasts);
    }

    return retval;
}

static inline  std::string StringTrim(const std::string & str) {
    int start = 0, end = str.length();

    for (; start < static_cast<int>(str.length()); ++start) {
        if (!std::isspace(str[start])) {
            break;
        }
    }

    for (; end > 0; --end) {
        if (!std::isspace(str[end - 1])) {
            break;
        }
    }

    return str.substr(start, end - start);
}

}
}
#endif /* _HDFS_LIBHDFS3_COMMON_STRINGUTIL_H_ */
