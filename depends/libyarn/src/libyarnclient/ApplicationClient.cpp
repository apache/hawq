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

#include <iostream>
#include <sstream>

#include "rpc/RpcAuth.h"
#include "common/XmlConfig.h"
#include "common/SessionConfig.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "ApplicationClient.h"
#include "StringUtil.h"

namespace libyarn {

RMInfo::RMInfo() {
}

const char * YARN_RESOURCEMANAGER_HA = "yarn.resourcemanager.ha";

std::vector<RMInfo> RMInfo::getHARMInfo(const Yarn::Config & conf, const char* name) {
    std::vector<RMInfo> retval;
    /*
    * Read config and create a vector of RM address.
    */
    try{
        std::string strHA = StringTrim(conf.getString(std::string(name)));
        std::vector<std::string> strRMs = StringSplit(strHA, ",");
        retval.resize(strRMs.size());
        for (size_t i = 0; i < strRMs.size(); ++i) {
            std::vector<std::string> rm = StringSplit(strRMs[i], ":");
            retval[i].setHost(rm[0]);
            retval[i].setPort(rm[1]);
        }
    } catch (const Yarn::YarnConfigNotFound &e) {
        LOG(DEBUG1, "Yarn RM HA is not configured.");
    }

    return retval;
}

ApplicationClient::ApplicationClient(string &user, string &host, string &port) {
    std::vector<RMInfo> rmConfInfos, rmInfos;
    RMInfo activeRM;
    std::string tokenService = "";

    Yarn::Internal::shared_ptr<Yarn::Config> conf = DefaultConfig().getConfig();
    Yarn::Internal::SessionConfig sessionConfig(*conf);
    RootLogger.setLogSeverity(sessionConfig.getLogSeverity());
    LOG(INFO, "ApplicationClient session auth method : %s",
            sessionConfig.getRpcAuthMethod().c_str());

    activeRM.setHost(host);
    activeRM.setPort(port);
    rmInfos.push_back(activeRM);
    rmConfInfos = RMInfo::getHARMInfo(*conf, YARN_RESOURCEMANAGER_HA);

    /* build a list of candidate RMs without duplicate */
    for (vector<RMInfo>::iterator it = rmConfInfos.begin();
            it != rmConfInfos.end(); it++) {
        bool found = false;
        for (vector<RMInfo>::iterator it2 = rmInfos.begin();
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
        LOG(INFO, "ApplicationClient Resource Manager HA is disable.");
        enableRMHA = false;
        maxRMHARetry = 0;
    } else {
        LOG(INFO,
                "ApplicationClient Resource Manager HA is enable. Number of RM: %d",
                rmInfos.size());
        enableRMHA = true;
        maxRMHARetry = sessionConfig.getRpcMaxHaRetry();
    }

    if (!enableRMHA)
    {
        appClientProtos.push_back(
            std::shared_ptr<ApplicationClientProtocol>(
                new ApplicationClientProtocol(user, host, port, tokenService, sessionConfig)));
    } else {
        /*
        * iterate RMInfo vector and create 1-1 applicationClientProtocol for each standby RM
        */
        for (size_t i = 0; i < rmInfos.size(); ++i) {
            appClientProtos.push_back(
                std::shared_ptr<ApplicationClientProtocol>(
                    new ApplicationClientProtocol(
                        user, rmInfos[i].getHost(),rmInfos[i].getPort(), tokenService, sessionConfig)));
            LOG(INFO, "ApplicationClient finds a candidate RM, host:%s, port:%s",
                      rmInfos[i].getHost().c_str(), rmInfos[i].getPort().c_str());
        }
    }
    currentAppClientProto = 0;
}

/*
 * Used for unittest
 */
ApplicationClient::ApplicationClient(ApplicationClientProtocol *appclient){
    appClientProtos.push_back(std::shared_ptr<ApplicationClientProtocol>(appclient));
    currentAppClientProto = 0;
}

ApplicationClient::~ApplicationClient() {
}

std::shared_ptr<ApplicationClientProtocol>
    ApplicationClient::getActiveAppClientProto(uint32_t & oldValue) {
    lock_guard<mutex> lock(this->mut);

    LOG(DEBUG2, "ApplicationClient::getActiveAppClientProto is called.");

    if (appClientProtos.empty()) {
        LOG(WARNING, "The vector of ApplicationClientProtocol is empty.");
        THROW(Yarn::YarnResourceManagerClosed, "ApplicationClientProtocol is closed.");
    }

    oldValue = currentAppClientProto;
    LOG(DEBUG1, "ApplicationClient::getActiveAppClientProto, current is %d.",
            currentAppClientProto);
    return appClientProtos[currentAppClientProto % appClientProtos.size()];
}

void ApplicationClient::failoverToNextAppClientProto(uint32_t oldValue){
    lock_guard<mutex> lock(mut);

    if (oldValue != currentAppClientProto || appClientProtos.size() == 1) {
        return;
    }

    ++currentAppClientProto;
    currentAppClientProto = currentAppClientProto % appClientProtos.size();
    LOG(INFO, "ApplicationClient::failoverToNextAppClientProto, current is %d.",
            currentAppClientProto);
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


#define RESOURCEMANAGER_HA_RETRY_BEGIN() \
    do { \
        int __count = 0; \
        do { \
            uint32_t __oldValue = 0; \
            std::shared_ptr<ApplicationClientProtocol> appClientProto = getActiveAppClientProto(__oldValue); \
            try { \
                (void)0

#define RESOURCEMANAGER_HA_RETRY_END() \
    break; \
    } catch (const Yarn::ResourceManagerStandbyException & e) { \
        if (!enableRMHA || __count++ > maxRMHARetry) { \
            throw; \
        } \
    } catch (const Yarn::YarnFailoverException & e) { \
        if (!enableRMHA || __count++ > maxRMHARetry) { \
            HandleYarnFailoverException(e); \
        } \
    } \
    failoverToNextAppClientProto(__oldValue); \
    } while (true); \
    } while (0)

/*
 rpc getNewApplication (GetNewApplicationRequestProto) returns (GetNewApplicationResponseProto);

 message GetNewApplicationRequestProto {
 }

 message GetNewApplicationResponseProto {
 optional ApplicationIdProto application_id = 1;
 optional ResourceProto maximumCapability = 2;
 }
 */
ApplicationId ApplicationClient::getNewApplication() {
    GetNewApplicationRequest request;
    GetNewApplicationResponse response;

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getNewApplication(request);
    RESOURCEMANAGER_HA_RETRY_END();
    return response.getApplicationId();
}

/*
 rpc submitApplication (SubmitApplicationRequestProto) returns (SubmitApplicationResponseProto);

 message SubmitApplicationRequestProto {
 optional ApplicationSubmissionContextProto application_submission_context= 1;
 }

 message SubmitApplicationResponseProto {
 }
 */

void ApplicationClient::submitApplication(
        ApplicationSubmissionContext &appContext) {
    SubmitApplicationRequest request;
    request.setApplicationSubmissionContext(appContext);

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    appClientProto->submitApplication(request);
    RESOURCEMANAGER_HA_RETRY_END();
}

/*
 rpc getApplicationReport (GetApplicationReportRequestProto) returns (GetApplicationReportResponseProto);

 message GetApplicationReportRequestProto {
 optional ApplicationIdProto application_id = 1;
 }

 message GetApplicationReportResponseProto {
 optional ApplicationReportProto application_report = 1;
 }
 */

ApplicationReport ApplicationClient::getApplicationReport(
        ApplicationId &appId) {
    GetApplicationReportRequest request;
    GetApplicationReportResponse response;

    request.setApplicationId(appId);
    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getApplicationReport(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getApplicationReport();
}

list<ContainerReport> ApplicationClient::getContainers(ApplicationAttemptId &appAttempId){
    GetContainersRequest request;
    GetContainersResponse response;

    request.setApplicationAttemptId(appAttempId);
    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getContainers(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getContainersReportList();
}

list<NodeReport> ApplicationClient::getClusterNodes(list<NodeState> &states) {
    GetClusterNodesRequest request;
    GetClusterNodesResponse response;
    request.setNodeStates(states);

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getClusterNodes(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getNodeReports();
}

QueueInfo ApplicationClient::getQueueInfo(string &queue, bool includeApps,
        bool includeChildQueues, bool recursive) {
    GetQueueInfoRequest request;
    GetQueueInfoResponse response;
    request.setQueueName(queue);
    request.setIncludeApplications(includeApps);
    request.setIncludeChildQueues(includeChildQueues);
    request.setRecursive(recursive);

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getQueueInfo(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getQueueInfo();
}

void ApplicationClient::forceKillApplication(ApplicationId &appId) {
    KillApplicationRequest request;
    request.setApplicationId(appId);

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    appClientProto->forceKillApplication(request);
    RESOURCEMANAGER_HA_RETRY_END();
}

YarnClusterMetrics ApplicationClient::getClusterMetrics() {
    GetClusterMetricsRequest request;
    GetClusterMetricsResponse response;

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getClusterMetrics(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getClusterMetrics();
}

list<ApplicationReport> ApplicationClient::getApplications(
        list<string> &applicationTypes,
        list<YarnApplicationState> &applicationStates) {
    GetApplicationsRequest request;
    GetApplicationsResponse response;
    request.setApplicationStates(applicationStates);
    request.setApplicationTypes(applicationTypes);

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getApplications(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getApplicationList();
}

list<QueueUserACLInfo> ApplicationClient::getQueueAclsInfo() {
    GetQueueUserAclsInfoRequest request;
    GetQueueUserAclsInfoResponse response;

    RESOURCEMANAGER_HA_RETRY_BEGIN();
    response = appClientProto->getQueueAclsInfo(request);
    RESOURCEMANAGER_HA_RETRY_END();

    return response.getUserAclsInfoList();
}

} /* namespace libyarn */
