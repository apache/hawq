/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Thread.h"

#include <cstring>
#include <cassert>
#include <sstream>

namespace Yarn {

function<bool(void)> ChecnOperationCanceledCallback;

namespace Internal {

bool CheckOperationCanceled() {
    if (ChecnOperationCanceledCallback && ChecnOperationCanceledCallback()) {
        THROW(YarnCanceled, "Operation has been canceled by the user.");
    }

    return false;
}

const char * GetSystemErrorInfo(int eno) {
    static THREAD_LOCAL char buffer[64];
    static THREAD_LOCAL char message[64];
    strerror_r(eno, buffer, sizeof(buffer));
    snprintf(message, sizeof(message), "(errno: %d) %s", eno, buffer);
    return message;
}

static THREAD_LOCAL std::string * MessageBuffer = NULL;
static THREAD_LOCAL once_flag once;

static void CreateMessageBuffer() {
    MessageBuffer = new std::string;
}

static void InitMessageBuffer() {
    call_once(once, &CreateMessageBuffer);
    assert(MessageBuffer != NULL);
}

static void GetExceptionDetailInternal(const Yarn::YarnException & e,
                                       std::stringstream & ss, bool topLevel);

static void GetExceptionDetailInternal(const std::exception & e,
                                       std::stringstream & ss, bool topLevel) {
    try {
        if (!topLevel) {
            ss << "Caused by\n";
        }

        ss << e.what();
    } catch (const std::bad_alloc & e) {
        return;
    }

    try {
    	Yarn::rethrow_if_nested(e);
    } catch (const Yarn::YarnException & e) {
        GetExceptionDetailInternal(e, ss, false);
    } catch (const std::exception & nested) {
        GetExceptionDetailInternal(e, ss, false);
    }
}

static void GetExceptionDetailInternal(const Yarn::YarnException & e,
                                       std::stringstream & ss, bool topLevel) {
    try {
        if (!topLevel) {
            ss << "Caused by\n";
        }

        ss << e.msg();
    } catch (const std::bad_alloc & e) {
        return;
    }

    try {
    	Yarn::rethrow_if_nested(e);
    } catch (const Yarn::YarnException & e) {
        GetExceptionDetailInternal(e, ss, false);
    } catch (const std::exception & nested) {
        GetExceptionDetailInternal(e, ss, false);
    }
}

const char * GetExceptionDetail(const Yarn::YarnException & e) {
    std::stringstream ss;
    GetExceptionDetailInternal(e, ss, true);

    try {
        InitMessageBuffer();
        *MessageBuffer = ss.str();
    } catch (const std::bad_alloc & e) {
        return "Out of memory";
    }

    return MessageBuffer->c_str();
}

const char * GetExceptionDetail(const exception_ptr e) {
    std::stringstream ss;

    try {
        InitMessageBuffer();
        Yarn::rethrow_exception(e);
    } catch (const Yarn::YarnException & nested) {
        GetExceptionDetailInternal(nested, ss, true);
    } catch (const std::exception & nested) {
        GetExceptionDetailInternal(nested, ss, true);
    }

    try {
        *MessageBuffer = ss.str();
    } catch (const std::bad_alloc & e) {
        return "Out of memory";
    }

    return MessageBuffer->c_str();
}

static void GetExceptionMessage(const std::exception & e,
                                std::stringstream & ss, int recursive) {
    try {
        for (int i = 0; i < recursive; ++i) {
            ss << '\t';
        }

        if (recursive > 0) {
            ss << "Caused by: ";
        }

        ss << e.what();
    } catch (const std::bad_alloc & e) {
        return;
    }

    try {
    		Yarn::rethrow_if_nested(e);
    } catch (const std::exception & nested) {
        GetExceptionMessage(nested, ss, recursive + 1);
    }
}

const char * GetExceptionMessage(const exception_ptr e, std::string & buffer) {
    std::stringstream ss;

    try {
    		Yarn::rethrow_exception(e);
    } catch (const std::bad_alloc & e) {
        return "Out of memory";
    } catch (const std::exception & e) {
        GetExceptionMessage(e, ss, 0);
    }

    try {
        buffer = ss.str();
    } catch (const std::bad_alloc & e) {
        return "Out of memory";
    }

    return buffer.c_str();
}

}
}

