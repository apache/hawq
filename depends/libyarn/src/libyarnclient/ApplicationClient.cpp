#include <iostream>
#include <sstream>

#include "rpc/RpcAuth.h"
#include "common/XmlConfig.h"
#include "common/SessionConfig.h"

#include "ApplicationClient.h"

namespace libyarn {

ApplicationClient::ApplicationClient(string &user, string &host, string &port) {
	std::string tokenService = "";
	Yarn::Internal::shared_ptr<Yarn::Config> conf = DefaultConfig().getConfig();
	Yarn::Internal::SessionConfig sessionConfig(*conf);
	LOG(INFO, "ApplicationClient session auth method : %s", sessionConfig.getRpcAuthMethod().c_str());
	appClient = (void*) new ApplicationClientProtocol(user, host, port, tokenService, sessionConfig);
}

ApplicationClient::ApplicationClient(ApplicationClientProtocol *appclient){
	appClient = (void*)appclient;
}

ApplicationClient::~ApplicationClient() {
	if (appClient != NULL){
		delete (ApplicationClientProtocol*) appClient;
	}
}

/*
 rpc getNewApplication (GetNewApplicationRequestProto) returns (GetNewApplicationResponseProto);

 message GetNewApplicationRequestProto {
 }

 message GetNewApplicationResponseProto {
 optional ApplicationIdProto application_id = 1;
 optional ResourceProto maximumCapability = 2;
 }
 */
ApplicationID ApplicationClient::getNewApplication() {
	ApplicationClientProtocol* appClientAlias =
			((ApplicationClientProtocol*) appClient);
	GetNewApplicationRequest request;
	GetNewApplicationResponse response = appClientAlias->getNewApplication(request);
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
	ApplicationClientProtocol* appClientAlias =
			((ApplicationClientProtocol*) appClient);
	SubmitApplicationRequest request;
	request.setApplicationSubmissionContext(appContext);
	appClientAlias->submitApplication(request);
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
		ApplicationID &appId) {
	ApplicationClientProtocol* appClientAlias =
			((ApplicationClientProtocol*) appClient);
	GetApplicationReportRequest request;
	request.setApplicationId(appId);
	GetApplicationReportResponse response =
			appClientAlias->getApplicationReport(request);
	/*ApplicationReport report = response.getApplicationReport();
	if (report.getYarnApplicationState() == YarnApplicationState::ACCEPTED) {
			Token token = report.getAMRMToken();
			LOG(INFO,"%s",report.getClientToAMToken().getIdentifier());
	}*/
	return response.getApplicationReport();
}

list<ContainerReport> ApplicationClient::getContainers(ApplicationAttemptId &appAttempId){
	ApplicationClientProtocol* appClientAlias =
			((ApplicationClientProtocol*) appClient);
	GetContainersRequest request;
	request.setApplicationAttemptId(appAttempId);
	/*LOG(INFO,
			"ApplicationClient::getContainers, appId[cluster_timestamp:%lld,id:%d]",
			request.getApplicationId().getClusterTimestamp(), request.getApplicationId().getId());
	*/
	GetContainersResponse response =
			appClientAlias->getContainers(request);
	return response.getcontainersReportList();
}

list<NodeReport> ApplicationClient::getClusterNodes(list<NodeState> &states) {
	GetClusterNodesRequest request;
	request.setNodeStates(states);
	GetClusterNodesResponse response =
			((ApplicationClientProtocol*) appClient)->getClusterNodes(request);
	return response.getNodeReports();
}

QueueInfo ApplicationClient::getQueueInfo(string &queue, bool includeApps,
		bool includeChildQueues, bool recursive) {
	GetQueueInfoRequest request;
	request.setQueueName(queue);
	request.setIncludeApplications(includeApps);
	request.setIncludeChildQueues(includeChildQueues);
	request.setRecursive(recursive);
	GetQueueInfoResponse response =
			((ApplicationClientProtocol*) appClient)->getQueueInfo(request);
	return response.getQueueInfo();
}

void ApplicationClient::forceKillApplication(ApplicationID &appId) {
	KillApplicationRequest request;
	request.setApplicationId(appId);
	((ApplicationClientProtocol*) appClient)->forceKillApplication(request);
}

YarnClusterMetrics ApplicationClient::getClusterMetrics() {
	GetClusterMetricsRequest request;
	GetClusterMetricsResponse response =
			((ApplicationClientProtocol*) appClient)->getClusterMetrics(
					request);
	return response.getClusterMetrics();
}

list<ApplicationReport> ApplicationClient::getApplications(
		list<string> &applicationTypes,
		list<YarnApplicationState> &applicationStates) {
	GetApplicationsRequest request;
	request.setApplicationStates(applicationStates);
	request.setApplicationTypes(applicationTypes);
	GetApplicationsResponse response =
			((ApplicationClientProtocol*) appClient)->getApplications(request);
	return response.getApplicationList();
}

list<QueueUserACLInfo> ApplicationClient::getQueueAclsInfo() {
	GetQueueUserAclsInfoRequest request;
	GetQueueUserAclsInfoResponse response =
			((ApplicationClientProtocol*) appClient)->getQueueAclsInfo(request);
	return response.getUserAclsInfoList();
}

} /* namespace libyarn */
