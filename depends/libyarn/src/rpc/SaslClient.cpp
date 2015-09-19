/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include <algorithm>
#include <cctype>

#include "Exception.h"
#include "ExceptionInternal.h"
#include "SaslClient.h"

#define SASL_SUCCESS 0

namespace Yarn {
namespace Internal {

SaslClient::SaslClient(const hadoop::common::RpcSaslProto_SaslAuth & auth, const Token & token,
                       const std::string & principal) :
    complete(false) {
    int rc;
    ctx = NULL;
    RpcAuth method = RpcAuth(RpcAuth::ParseMethod(auth.method()));
    rc = gsasl_init(&ctx);

    if (rc != GSASL_OK) {
        THROW(YarnIOException, "cannot initialize libgsasl");
    }

    switch (method.getMethod()) {
    case AuthMethod::KERBEROS:
        initKerberos(auth, principal);
        break;

    case AuthMethod::TOKEN:
        initDigestMd5(auth, token);
        break;

    default:
        THROW(YarnIOException, "unknown auth method.");
        break;
    }
}

SaslClient::~SaslClient() {
    if (session != NULL) {
        gsasl_finish(session);
    }

    if (ctx != NULL) {
        gsasl_done(ctx);
    }
}

void SaslClient::initKerberos(const hadoop::common::RpcSaslProto_SaslAuth & auth,
                              const std::string & principal) {
    int rc;

    /* Create new authentication session. */
    if ((rc = gsasl_client_start(ctx, auth.mechanism().c_str(), &session)) != GSASL_OK) {
        THROW(YarnIOException, "Cannot initialize client (%d): %s", rc,
              gsasl_strerror(rc));
    }

    gsasl_property_set(session, GSASL_SERVICE, auth.protocol().c_str());
    gsasl_property_set(session, GSASL_AUTHID, principal.c_str());
    gsasl_property_set(session, GSASL_HOSTNAME, auth.serverid().c_str());
}

std::string Base64Encode(const std::string & in) {
    char * temp;
    size_t len;
    std::string retval;
    int rc = gsasl_base64_to(in.c_str(), in.size(), &temp, &len);

    if (rc != GSASL_OK) {
        throw std::bad_alloc();
    }

    if (temp) {
        retval = temp;
        free(temp);
    }

    if (!temp || retval.length() != len) {
        THROW(YarnIOException, "SaslClient: Failed to encode string to base64");
    }

    return retval;
}

void SaslClient::initDigestMd5(const hadoop::common::RpcSaslProto_SaslAuth & auth,
                               const Token & token) {
    int rc;

    if ((rc = gsasl_client_start(ctx, auth.mechanism().c_str(), &session)) != GSASL_OK) {
        THROW(YarnIOException, "Cannot initialize client (%d): %s", rc, gsasl_strerror(rc));
    }

    std::string password = Base64Encode(token.getPassword());
    std::string identifier = Base64Encode(token.getIdentifier());
    gsasl_property_set(session, GSASL_PASSWORD, password.c_str());
    gsasl_property_set(session, GSASL_AUTHID, identifier.c_str());
    gsasl_property_set(session, GSASL_HOSTNAME, auth.serverid().c_str());
    gsasl_property_set(session, GSASL_SERVICE, auth.protocol().c_str());
}

std::string SaslClient::evaluateChallenge(const std::string & challenge) {
    int rc;
    char * output = NULL;
    size_t outputSize;
    std::string retval;
    rc = gsasl_step(session, &challenge[0], challenge.size(), &output,
                    &outputSize);

    if (rc == GSASL_NEEDS_MORE || rc == GSASL_OK) {
        retval.resize(outputSize);
        memcpy(&retval[0], output, outputSize);

        if (output) {
            free(output);
        }
    } else {
        THROW(AccessControlException, "Failed to evaluate challenge: %s", gsasl_strerror(rc));
    }

    if (rc == GSASL_OK) {
        complete = true;
    }

    return retval;
}

bool SaslClient::isComplete() {
    return complete;
}

std::string SaslClient::getQOP() {
    const char * retval = gsasl_property_get(session, GSASL_QOP);
    std::string qop = retval == NULL ? "" : retval;
    std::transform(qop.begin(), qop.end(), qop.begin(), ::tolower);

    if (qop.find("auth") != qop.npos) {
        return "auth";
    }

    if (qop.find("int") != qop.npos) {
        return "int";
    }

    if (qop.find("conf") != qop.npos) {
        return "conf";
    }

    return "auth";
}

}
}

