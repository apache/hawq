/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/

#include <unistd.h>

#include "Thread.h"

namespace Yarn {
namespace Internal {

sigset_t ThreadBlockSignal() {
    sigset_t sigs;
    sigset_t oldMask;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGHUP);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    sigaddset(&sigs, SIGUSR1);
    sigaddset(&sigs, SIGUSR2);
    pthread_sigmask(SIG_BLOCK, &sigs, &oldMask);
    return oldMask;
}

void ThreadUnBlockSignal(sigset_t sigs) {
    pthread_sigmask(SIG_SETMASK, &sigs, 0);
}

}
}
