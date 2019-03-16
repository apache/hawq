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

#include <pthread.h>
#include <vector>

#include "rpc/RpcAuth.h"
#include "common/XmlConfig.h"
#include "common/SessionConfig.h"

#include "ApplicationMaster.h"
#include "ApplicationClient.h"

namespace libyarn {

const char * YARN_RESOURCEMANAGER_SCHEDULER_HA = "yarn.resourcemanager.scheduler.ha";

ApplicationMaster::ApplicationMaster(string &schedHost, string &schedPort,
        UserInfo &user, const string &tokenService) {
    std::vector<RMInfo> rmInfos, rmConfInfos;
    Yarn::Internal::shared_ptr<Yarn::Config> conf = DefaultConfig().getConfig();
    Yarn::Internal::SessionConfig sessionConfig(*conf);
    RpcAuth rpcAuth(user, AuthMethod::TOKEN);

    RMInfo activeRM;
    activeRM.setHost(schedHost);
    activeRM.setPort(schedPort);
    rmInfos.push_back(activeRM);
    rmConfInfos = RMInfo::getHARMInfo(*conf, YARN_RESOURCEMANAGER_SCHEDULER_HA);

    /* build a list of candidate RMs without duplicate */
    for (std::vector<RMInfo>::iterator it = rmConfInfos.begin();
            it != rmConfInfos.end(); it++) {
        bool found = false;
        for (std::vector<RMInfo>::iterator it2 = rmInfos.begin();
                it2 != rmInfos.end(); it2++) {
            if (it2->getHost() == it->getHost()
                    && it2->getPort() == it->getPort()) {
                found = true;
                break;
            }
        }
        if (!found)
            rmInfos.push_back(*it);
    }

    if (rmInfos.size() <= 1) {
        LOG(INFO, "ApplicationClient RM Scheduler HA is disable.");
        enableRMSchedulerHA = false;
        maxRMHARetry = 0;
    } else {
        LOG(INFO,
                "ApplicationClient RM Scheduler HA is enable. Number of RM scheduler: %d",
                rmInfos.size());
        enableRMSchedulerHA = true;
        maxRMHARetry = sessionConfig.getRpcMaxHaRetry();
    }

    if (!enableRMSchedulerHA)
    {
        appMasterProtos.push_back(
            std::shared_ptr<ApplicationMasterProtocol>(
                new ApplicationMasterProtocol(schedHost, schedPort, tokenService, sessionConfig, rpcAuth)));
    }
    else {
        /*
         * iterate RMInfo vector and create 1-1 applicationMasterProtocol for each standby RM scheduler.
         */
        for (size_t i = 0; i < rmInfos.size(); ++i) {
            appMasterProtos.push_back(
                std::shared_ptr<ApplicationMasterProtocol>(
                    new ApplicationMasterProtocol(rmInfos[i].getHost(),
                        rmInfos[i].getPort(), tokenService, sessionConfig, rpcAuth)));
            LOG(INFO,
                    "ApplicationMaster finds a candidate RM scheduler, host:%s, port:%s",
                    rmInfos[i].getHost().c_str(), rmInfos[i].getPort().c_str());
        }
    }
    currentAppMasterProto = 0;
}

ApplicationMaster::ApplicationMaster(ApplicationMasterProtocol *rmclient){
    appMasterProtos.push_back(std::shared_ptr<ApplicationMasterProtocol>(rmclient));
    currentAppMasterProto = 0;
}

ApplicationMaster::~ApplicationMaster() {
}

std::shared_ptr<ApplicationMasterProtocol>
    ApplicationMaster::getActiveAppMasterProto(uint32_t & oldValue) {
    lock_guard<mutex> lock(this->mut);

    if (appMasterProtos.empty()) {
        LOG(WARNING, "The vector of ApplicationMasterProtocol is empty.");
        THROW(Yarn::YarnResourceManagerClosed, "ApplicationMasterProtocol is closed.");
    }

    oldValue = currentAppMasterProto;
    LOG(DEBUG2, "ApplicationMaster::getActiveAppMasterProto, current is %d.",
            currentAppMasterProto);
    return appMasterProtos[currentAppMasterProto % appMasterProtos.size()];
}

void ApplicationMaster::failoverToNextAppMasterProto(uint32_t oldValue){
    lock_guard<mutex> lock(mut);

    if (oldValue != currentAppMasterProto || appMasterProtos.size() == 1) {
        return;
    }

    ++currentAppMasterProto;
    currentAppMasterProto = currentAppMasterProto % appMasterProtos.size();
    LOG(INFO, "ApplicationMaster::failoverToNextAppMasterProto, current is %d.",
            currentAppMasterProto);
}

static void HandleYarnFailoverException(const Yarn::YarnFailoverException & e) {
    try {
        Yarn::rethrow_if_nested(e);
    } catch (...) {
        NESTED_THROW(Yarn::YarnRpcException, "%s", e.what());
    }

    //should not reach here
    abort();
}

#define RESOURCEMANAGER_SCHEDULER_HA_RETRY_BEGIN() \
    do { \
        int __count = 0; \
        do { \
            uint32_t __oldValue = 0; \
            std::shared_ptr<ApplicationMasterProtocol> appMasterProto = getActiveAppMasterProto(__oldValue); \
            try { \
                (void)0

#define RESOURCEMANAGER_SCHEDULER_HA_RETRY_END() \
    break; \
    } catch (const Yarn::ResourceManagerStandbyException & e) { \
        if (!enableRMSchedulerHA || __count++ > maxRMHARetry) { \
            throw; \
        } \
    } catch (const Yarn::YarnFailoverException & e) { \
        if (!enableRMSchedulerHA || __count++ > maxRMHARetry) { \
            HandleYarnFailoverException(e); \
        } \
    } \
    failoverToNextAppMasterProto(__oldValue); \
    } while (true); \
    } while (0)

/*
rpc registerApplicationMaster (RegisterApplicationMasterRequestProto) returns (RegisterApplicationMasterResponseProto);

message RegisterApplicationMasterRequestProto {
  optional string host = 1;
  optional int32 rpc_port = 2;
  optional string tracking_url = 3;
}

message RegisterApplicationMasterResponseProto {
  optional ResourceProto maximumCapability = 1;
  optional bytes client_to_am_token_master_key = 2;
  repeated ApplicationACLMapProto application_ACLs = 3;
}
 */
RegisterApplicationMasterResponse ApplicationMaster::registerApplicationMaster(
    string &amHost, int32_t amPort, string &am_tracking_url) {
    RegisterApplicationMasterRequest request;
    RegisterApplicationMasterResponse response;
    request.setHost(amHost);
    request.setRpcPort(amPort);
    request.setTrackingUrl(am_tracking_url);

    RESOURCEMANAGER_SCHEDULER_HA_RETRY_BEGIN();
    response = appMasterProto->registerApplicationMaster(request);
    RESOURCEMANAGER_SCHEDULER_HA_RETRY_END();
    return response;
}

/*
message AllocateRequestProto {
  repeated ResourceRequestProto ask = 1;
  repeated ContainerIdProto release = 2;
  optional ResourceBlacklistRequestProto blacklist_request = 3;
  optional int32 response_id = 4;
  optional float progress = 5;
}

message AllocateResponseProto {
  optional AMCommandProto a_m_command = 1;
  optional int32 response_id = 2;
  repeated ContainerProto allocated_containers = 3;
  repeated ContainerStatusProto completed_container_statuses = 4;
  optional ResourceProto limit = 5;
  repeated NodeReportProto updated_nodes = 6;
  optional int32 num_cluster_nodes = 7;
  optional PreemptionMessageProto preempt = 8;
  repeated NMTokenProto nm_tokens = 9;
}
*/

AllocateResponse ApplicationMaster::allocate(list<ResourceRequest> &asks,
        list<ContainerId> &releases, ResourceBlacklistRequest &blacklistRequest,
        int32_t responseId, float progress) {
    AllocateRequest request;
    AllocateResponse response;
    request.setAsks(asks);
    request.setReleases(releases);
    request.setBlacklistRequest(blacklistRequest);
    request.setResponseId(responseId);
    request.setProgress(progress);

    RESOURCEMANAGER_SCHEDULER_HA_RETRY_BEGIN();
    response = appMasterProto->allocate(request);
    RESOURCEMANAGER_SCHEDULER_HA_RETRY_END();
    return response;
}

/*
rpc finishApplicationMaster (FinishApplicationMasterRequestProto) returns (FinishApplicationMasterResponseProto);

message FinishApplicationMasterRequestProto {
  optional string diagnostics = 1;
  optional string tracking_url = 2;
  optional FinalApplicationStatusProto final_application_status = 3;
}

message FinishApplicationMasterResponseProto {
  optional bool isUnregistered = 1 [default = false];
}
*/

bool ApplicationMaster::finishApplicationMaster(string &diagnostics,
        string &trackingUrl, FinalApplicationStatus finalstatus) {
    FinishApplicationMasterRequest request;
    FinishApplicationMasterResponse response;
    request.setDiagnostics(diagnostics);
    request.setTrackingUrl(trackingUrl);
    request.setFinalApplicationStatus(finalstatus);

    RESOURCEMANAGER_SCHEDULER_HA_RETRY_BEGIN();
    response = appMasterProto->finishApplicationMaster(request);
    RESOURCEMANAGER_SCHEDULER_HA_RETRY_END();
    return response.getIsUnregistered();
}

} /* namespace libyarn */
