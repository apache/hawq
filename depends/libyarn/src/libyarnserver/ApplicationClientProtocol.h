#ifndef APPLICATIONCLIENTPROTOCOL_H_
#define APPLICATIONCLIENTPROTOCOL_H_

#include "rpc/RpcAuth.h"
#include "rpc/RpcClient.h"
#include "rpc/RpcConfig.h"
#include "rpc/RpcProtocolInfo.h"
#include "rpc/RpcServerInfo.h"
#include "rpc/RpcChannel.h"
#include "common/SessionConfig.h"

#include "YARN_applicationclient_protocol.pb.h"

#include "protocolrecords/GetNewApplicationRequest.h"
#include "protocolrecords/GetNewApplicationResponse.h"
#include "protocolrecords/SubmitApplicationRequest.h"
#include "protocolrecords/GetApplicationReportRequest.h"
#include "protocolrecords/GetApplicationReportResponse.h"
#include "protocolrecords/GetContainersRequest.h"
#include "protocolrecords/GetContainersResponse.h"
#include "protocolrecords/GetClusterNodesRequest.h"
#include "protocolrecords/GetClusterNodesResponse.h"
#include "protocolrecords/GetQueueInfoRequest.h"
#include "protocolrecords/GetQueueInfoResponse.h"
#include "protocolrecords/KillApplicationRequest.h"
#include "protocolrecords/KillApplicationResponse.h"
#include "protocolrecords/GetClusterMetricsRequest.h"
#include "protocolrecords/GetClusterMetricsResponse.h"
#include "protocolrecords/GetApplicationsRequest.h"
#include "protocolrecords/GetApplicationsResponse.h"
#include "protocolrecords/GetQueueUserAclsInfoRequest.h"
#include "protocolrecords/GetQueueUserAclsInfoResponse.h"

#define APP_CLIENT_PROTOCOL_VERSION 1
#define APP_CLIENT_PROTOCOL "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
#define APP_CLIENT_DELEGATION_TOKEN_KIND "ContainerToken"

using namespace std;
using namespace google::protobuf;
using namespace hadoop::yarn;
using namespace Yarn::Internal;

namespace libyarn {

class ApplicationClientProtocol {
public:
	ApplicationClientProtocol(const string &user, const string &rmHost, const string &rmPort,
			const string &tokenService, const SessionConfig &c);

	virtual ~ApplicationClientProtocol();

	virtual GetNewApplicationResponse getNewApplication(
			GetNewApplicationRequest &request);

	virtual void submitApplication(SubmitApplicationRequest &request);

	virtual GetApplicationReportResponse getApplicationReport(
			GetApplicationReportRequest &request);

	virtual GetContainersResponse getContainers(GetContainersRequest &request);

	virtual GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest &request);

	virtual GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest &request);

	virtual KillApplicationResponse forceKillApplication(
			KillApplicationRequest &request);

	virtual GetClusterMetricsResponse getClusterMetrics(
			GetClusterMetricsRequest &request);

	virtual GetApplicationsResponse getApplications(GetApplicationsRequest &request);

	virtual GetQueueUserAclsInfoResponse getQueueAclsInfo(GetQueueUserAclsInfoRequest &request);

	const string & getUser() {return auth.getUser().getRealUser();};

	AuthMethod getMethod() {return auth.getMethod();};

	const string getPrincipal() {return auth.getUser().getPrincipal();};

private:
	void invoke(const RpcCall & call);

private:
	RpcAuth auth;
	RpcClient & client;
	RpcConfig conf;
	RpcProtocolInfo protocol;
	RpcServerInfo server;

	RpcChannel *channel;
};

}
#endif /* APPLICATIONCLIENTPROTOCOL_H_ */
