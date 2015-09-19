/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_COMMON_THREAD_H_
#define _HDFS_LIBHDFS3_COMMON_THREAD_H_

#include "platform.h"

#include <signal.h>

#ifdef NEED_BOOST

#include <boost/thread.hpp>

namespace Yarn {
namespace Internal {

using boost::thread;
using boost::mutex;
using boost::lock_guard;
using boost::unique_lock;
using boost::condition_variable;
using boost::defer_lock_t;
using boost::once_flag;
using boost::call_once;
using namespace boost::this_thread;

}
}

#else

#include <thread>
#include <mutex>
#include <condition_variable>

namespace Yarn {
namespace Internal {

using std::thread;
using std::mutex;
using std::lock_guard;
using std::unique_lock;
using std::condition_variable;
using std::defer_lock_t;
using std::once_flag;
using std::call_once;
using namespace std::this_thread;

}
}
#endif

namespace Yarn {
namespace Internal {

/*
 * make the background thread ignore these signals (which should allow that
 * they be delivered to the main thread)
 */
sigset_t ThreadBlockSignal();

/*
 * Restore previous signals.
 */
void ThreadUnBlockSignal(sigset_t sigs);

}
}

#define CREATE_THREAD(retval, fun) \
    do { \
        sigset_t sigs = Yarn::Internal::ThreadBlockSignal(); \
        try { \
            retval = Yarn::Internal::thread(fun); \
            Yarn::Internal::ThreadUnBlockSignal(sigs); \
        } catch (...) { \
        	Yarn::Internal::ThreadUnBlockSignal(sigs); \
            throw; \
        } \
    } while(0)

#endif /* _HDFS_LIBHDFS3_COMMON_THREAD_H_ */
