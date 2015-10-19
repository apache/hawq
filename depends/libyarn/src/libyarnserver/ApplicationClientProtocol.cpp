#include <iostream>

#include "rpc/RpcCall.h"
#include "rpc/RpcChannel.h"
#include "rpc/RpcClient.h"
#include "libyarncommon/RpcHelper.h"

#include "common/Exception.h"
#include "common/ExceptionInternal.h"

#include "ApplicationClientProtocol.h"


using namespace Yarn;

namespace libyarn {

ApplicationClientProtocol::ApplicationClientProtocol(const string &rmUser,
			const string & rmHost, const string & rmPort,
			const string & tokenService,const SessionConfig & c) :
			client(RpcClient::getClient()), conf(c),
			protocol(APP_CLIENT_PROTOCOL_VERSION, APP_CLIENT_PROTOCOL,APP_CLIENT_DELEGATION_TOKEN_KIND),
			server(tokenService, rmHost, rmPort) {

	/* create RpcAuth for rpc method,
	 * can be SIMPLE or KERBEROS
	 * */
	if (RpcAuth::ParseMethod(c.getRpcAuthMethod()) == KERBEROS) {
		/*
		 * If using KERBEROS, rmUser should be principal name.
		 */
		Yarn::Internal::UserInfo user(rmUser);
		user.setRealUser(user.getEffectiveUser());
		Yarn::Internal::RpcAuth rpcAuth(user, KERBEROS);
		auth = rpcAuth;
	} else {
		Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
		Yarn::Internal::RpcAuth rpcAuth(user, SIMPLE);
		auth = rpcAuth;
	}
}

ApplicationClientProtocol::~ApplicationClientProtocol() {
}

void ApplicationClientProtocol::invoke(const RpcCall & call) {
	try {
		channel = &client.getChannel(auth, protocol, server, conf);
		channel->invoke(call);
		channel->close(false);
	}
	catch (...) {
		channel->close(false);
		throw;
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

GetNewApplicationResponse ApplicationClientProtocol::getNewApplication(
		GetNewApplicationRequest &request) {
	try {
		GetNewApplicationResponseProto responseProto;
		GetNewApplicationRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getNewApplication", &requestProto, &responseProto));
		return GetNewApplicationResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getNewApplication in %s: %d",
			  __FILE__, __LINE__);
	}
}

/*
 rpc submitApplication (SubmitApplicationRequestProto) returns (SubmitApplicationResponseProto);

 message SubmitApplicationRequestProto {
 optional ApplicationSubmissionContextProto application_submission_context= 1;
 }

 message SubmitApplicationResponseProto {
 }
 */

void ApplicationClientProtocol::submitApplication(
		SubmitApplicationRequest &request) {
	try {
		SubmitApplicationResponseProto responseProto;
		SubmitApplicationRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "submitApplication", &requestProto, &responseProto));
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::submitApplication in %s: %d",
			  __FILE__, __LINE__);
	}
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
GetApplicationReportResponse ApplicationClientProtocol::getApplicationReport(
		GetApplicationReportRequest &request) {
	try {
		GetApplicationReportResponseProto responseProto;
		GetApplicationReportRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getApplicationReport", &requestProto, &responseProto));
		return GetApplicationReportResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getApplicationReport in %s: %d",
			  __FILE__, __LINE__);
	}
}

/*
 rpc getContainers (GetContainersRequestProto) returns (GetContainersResponseProto);

message GetContainersRequestProto {
  optional ApplicationIdProto application_id = 1;
}

message GetContainersResponseProto {
  repeated ContainerReportProto containers_reports = 1;
}
 */
GetContainersResponse ApplicationClientProtocol::getContainers(GetContainersRequest &request){
	try {
		GetContainersResponseProto responseProto;
		GetContainersRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getContainers", &requestProto,&responseProto));
		return GetContainersResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getContainers in %s: %d",
			  __FILE__, __LINE__);
	}
}

/*
 rpc getClusterNodes (GetClusterNodesRequestProto) returns (GetClusterNodesResponseProto);

 message GetClusterNodesRequestProto {
 repeated NodeStateProto nodeStates = 1;
 }

 message GetClusterNodesResponseProto {
 repeated NodeReportProto nodeReports = 1;
 }
 */
GetClusterNodesResponse ApplicationClientProtocol::getClusterNodes(
		GetClusterNodesRequest &request) {
	try {
		GetClusterNodesResponseProto responseProto;
		GetClusterNodesRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getClusterNodes", &requestProto, &responseProto));
		return GetClusterNodesResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getClusterNodes in %s: %d",
			  __FILE__, __LINE__);
	}
}

/*
 rpc getQueueInfo (GetQueueInfoRequestProto) returns (GetQueueInfoResponseProto);

 message GetQueueInfoRequestProto {
 optional string queueName = 1;
 optional bool includeApplications = 2;
 optional bool includeChildQueues = 3;
 optional bool recursive = 4;
 }

 message GetQueueInfoResponseProto {
 optional QueueInfoProto queueInfo = 1;
 }

 message QueueInfoProto {
 optional string queueName = 1;
 optional float capacity = 2;
 optional float maximumCapacity = 3;
 optional float currentCapacity = 4;
 optional QueueStateProto state = 5;
 repeated QueueInfoProto childQueues = 6;
 repeated ApplicationReportProto applications = 7;
 }
 */
GetQueueInfoResponse ApplicationClientProtocol::getQueueInfo(
		GetQueueInfoRequest &request) {
	try {
		GetQueueInfoResponseProto responseProto;
		GetQueueInfoRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getQueueInfo", &requestProto, &responseProto));
		return GetQueueInfoResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getQueueInfo in %s: %d",
			  __FILE__, __LINE__);
	}
}

//rpc getClusterMetrics (GetClusterMetricsRequestProto) returns (GetClusterMetricsResponseProto);

//message GetClusterMetricsRequestProto {
//}

GetClusterMetricsResponse ApplicationClientProtocol::getClusterMetrics(
		GetClusterMetricsRequest &request) {
	try {
		GetClusterMetricsResponseProto responseProto;
		GetClusterMetricsRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getClusterMetrics", &requestProto, &responseProto));
		return GetClusterMetricsResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getClusterMetrics in %s: %d",
			  __FILE__, __LINE__);
	}
}

KillApplicationResponse ApplicationClientProtocol::forceKillApplication(
		KillApplicationRequest &request) {
	try {
		KillApplicationResponseProto responseProto;
		KillApplicationRequestProto requestProto = request.getProto();
		invoke(
				RpcCall(true, "forceKillApplication", &requestProto,
						&responseProto));
		return KillApplicationResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::forceKillApplication in %s: %d",
			  __FILE__, __LINE__);
	}
}

GetApplicationsResponse ApplicationClientProtocol::getApplications(
		GetApplicationsRequest &request) {
	try {
		GetApplicationsResponseProto responseProto;
		GetApplicationsRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getApplications", &requestProto, &responseProto));
		return GetApplicationsResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getApplications in %s: %d",
			  __FILE__, __LINE__);
	}
}

GetQueueUserAclsInfoResponse ApplicationClientProtocol::getQueueAclsInfo(
		GetQueueUserAclsInfoRequest &request) {
	try {
		GetQueueUserAclsInfoResponseProto responseProto;
		GetQueueUserAclsInfoRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "getQueueUserAcls", &requestProto, &responseProto));
		return GetQueueUserAclsInfoResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationClientProtocol::getQueueAclsInfo in %s: %d",
			  __FILE__, __LINE__);
	}
}

}

