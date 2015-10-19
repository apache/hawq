/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "Exception.h"

#include <sstream>

namespace Yarn {

const char * YarnIOException::ReflexName = "java.io.IOException";

const char * AlreadyBeingCreatedException::ReflexName =
    "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException";

const char * AccessControlException::ReflexName =
    "org.apache.hadoop.security.AccessControlException";

const char * FileAlreadyExistsException::ReflexName =
    "org.apache.hadoop.fs.FileAlreadyExistsException";

const char * DSQuotaExceededException::ReflexName =
    "org.apache.hadoop.hdfs.protocol.DSQuotaExceededException";

const char * NSQuotaExceededException::ReflexName =
    "org.apache.hadoop.hdfs.protocol.NSQuotaExceededException";

const char * ParentNotDirectoryException::ReflexName =
    "org.apache.hadoop.fs.ParentNotDirectoryException";

const char * SafeModeException::ReflexName =
    "org.apache.hadoop.hdfs.server.namenode.SafeModeException";

const char * NotReplicatedYetException::ReflexName =
    "org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException";

const char * FileNotFoundException::ReflexName = "java.io.FileNotFoundException";

const char * UnresolvedLinkException::ReflexName =
    "org.apache.hadoop.fs.UnresolvedLinkException";

const char * UnsupportedOperationException::ReflexName =
    "java.lang.UnsupportedOperationException";

const char * ReplicaNotFoundException::ReflexName =
    "org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException";

const char * ResourceManagerStandbyException::ReflexName =
    "org.apache.hadoop.ipc.StandbyException";

const char * YarnInvalidBlockToken::ReflexName =
    "org.apache.hadoop.security.token.SecretManager$InvalidToken";

const char * SaslException::ReflexName = "javax.security.sasl.SaslException";

YarnException::YarnException(const std::string & arg, const char * file,
                             int line, const char * stack) :
    std::runtime_error(arg) {
    std::ostringstream ss;
    ss << file << ": " << line << ": " << arg << std::endl << stack;
    detail = ss.str();
}
}
