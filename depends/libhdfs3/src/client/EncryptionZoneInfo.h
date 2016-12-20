/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_ENCRYPTIONZONEINFO_H_
#define _HDFS_LIBHDFS3_CLIENT_ENCRYPTIONZONEINFO_H_

#include <string>

namespace Hdfs {

class EncryptionZoneInfo {
public:
    EncryptionZoneInfo() : 
		suite(0), cryptoProtocolVersion(0), id(0) {
    }

    int getSuite() const {
        return suite;
    }

    void setSuite(int suite) {
        this->suite = suite;
    }

    int getCryptoProtocolVersion() const {
        return cryptoProtocolVersion;
    }

    void setCryptoProtocolVersion(int cryptoProtocolVersion) {
        this->cryptoProtocolVersion = cryptoProtocolVersion;
    }

    int getId() const {
        return id;
    }

    void setId(int id) {
        this->id = id;
    }

    const char * getPath() const{
        return path.c_str();
    }

    void setPath(const char * path){
        this->path = path;
    }

    const char * getKeyName() const{
        return keyName.c_str();
    }

    void setKeyName(const char * keyName){
        this->keyName = keyName;
    }

private:
    int suite;
    int cryptoProtocolVersion;
    int64_t id;
    std::string path;
    std::string keyName;
};

}
#endif
