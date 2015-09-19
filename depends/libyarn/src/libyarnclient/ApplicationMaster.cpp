#include <pthread.h>

#include "rpc/RpcAuth.h"
#include "common/XmlConfig.h"
#include "common/SessionConfig.h"

#include "ApplicationMaster.h"

namespace libyarn {
ApplicationMaster::ApplicationMaster(string &schedHost, string &schedPort,
		UserInfo &user, const string &tokenService) {
	Config config;
	SessionConfig sessionConfig(config);
	RpcAuth rpcAuth(user, AuthMethod::TOKEN);
	rmClient = (void*) new ApplicationMasterProtocol(schedHost,
			schedPort, tokenService, sessionConfig, rpcAuth);
}

ApplicationMaster::ApplicationMaster(ApplicationMasterProtocol *rmclient){
	rmClient = (void*)rmclient;
}


ApplicationMaster::~ApplicationMaster() {
	delete (ApplicationMasterProtocol*) rmClient;
}

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
	request.setHost(amHost);
	request.setRpcPort(amPort);
	request.setTrackingUrl(am_tracking_url);
	ApplicationMasterProtocol* rmClientAlias = (ApplicationMasterProtocol*) rmClient;
	return rmClientAlias->registerApplicationMaster(request);
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
	request.setAsks(asks);
	request.setReleases(releases);
	request.setBlacklistRequest(blacklistRequest);
	request.setResponseId(responseId);
	request.setProgress(progress);

	return ((ApplicationMasterProtocol*) rmClient)->allocate(request);
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
	ApplicationMasterProtocol* rmClientAlias = (ApplicationMasterProtocol*) rmClient;

	FinishApplicationMasterRequest request;
	request.setDiagnostics(diagnostics);
	request.setTrackingUrl(trackingUrl);
	request.setFinalApplicationStatus(finalstatus);

	FinishApplicationMasterResponse response = rmClientAlias->finishApplicationMaster(request);

	return response.getIsUnregistered();
}

} /* namespace libyarn */
