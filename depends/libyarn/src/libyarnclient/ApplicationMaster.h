/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef APPLICATIONMASTER_H_
#define APPLICATIONMASTER_H_

#include <list>

#include "libyarncommon/UserInfo.h"

#include "libyarnserver/ApplicationMasterProtocol.h"

#include "protocolrecords/RegisterApplicationMasterRequest.h"
#include "protocolrecords/RegisterApplicationMasterResponse.h"
#include "protocolrecords/AllocateRequest.h"
#include "protocolrecords/AllocateResponse.h"

#include "records/ResourceRequest.h"
#include "records/ContainerId.h"
#include "records/ResourceBlacklistRequest.h"


using std::string; using std::list;
using namespace Yarn::Internal;
using namespace Yarn;

namespace libyarn {

class ApplicationMaster {
public:
    ApplicationMaster(string &schedHost, string &schedPort,
            UserInfo &user, const string &tokenService);

    virtual ~ApplicationMaster();

    ApplicationMaster(ApplicationMasterProtocol *rmclient);

    virtual RegisterApplicationMasterResponse registerApplicationMaster(string &amHost,
            int32_t amPort, string &am_tracking_url);

    virtual AllocateResponse allocate(list<ResourceRequest> &asks,
            list<ContainerId> &releases,
            ResourceBlacklistRequest &blacklistRequest, int32_t responseId,
            float progress);

    virtual bool finishApplicationMaster(string &diagnostics, string &trackingUrl,
            FinalApplicationStatus finalstatus);
private:
    std::shared_ptr<ApplicationMasterProtocol> getActiveAppMasterProto(uint32_t & oldValue);
    void failoverToNextAppMasterProto(uint32_t oldValue);

private:
    bool enableRMSchedulerHA;
    int maxRMHARetry;
    mutex mut;
    /**
     * Each ApplicationMasterProto object stands for a connection to a standby RM scheduler.
     * If application master fail in connecting the active RM scheduler, it will try the
     * next one in the list.
     */
    std::vector<std::shared_ptr<ApplicationMasterProtocol>> appMasterProtos;
    uint32_t currentAppMasterProto;
};

} /* namespace libyarn */

#endif /* APPLICATIONMASTER_H_ */
