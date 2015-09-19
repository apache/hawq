/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_LOGGER_H_
#define _HDFS_LIBHDFS3_COMMON_LOGGER_H_

#define DEFAULT_LOG_LEVEL INFO

namespace Yarn {
namespace Internal {

extern const char * SeverityName[7];

enum LogSeverity {
    FATAL, LOG_ERROR, WARNING, INFO, DEBUG1, DEBUG2, DEBUG3
};

class Logger;

class Logger {
public:
    Logger();

    ~Logger();

    void setOutputFd(int f);

    void setLogSeverity(LogSeverity l);

    void printf(LogSeverity s, const char * fmt, ...) __attribute__((format(printf, 3, 4)));

private:
    int fd;
    LogSeverity severity;
};

extern Logger RootLogger;

}
}

#define LOG(s, fmt, ...) \
		Yarn::Internal::RootLogger.printf(s, fmt, ##__VA_ARGS__)

#endif /* _HDFS_LIBHDFS3_COMMON_LOGGER_H_ */
