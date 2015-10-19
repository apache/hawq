/*
 * ApplicationMasterProtocol.cpp
 *
 *  Created on: Jun 15, 2014
 *      Author: bwang
 */

#include "ApplicationMasterProtocol.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "rpc/RpcCall.h"
#include "rpc/RpcChannel.h"
#include "libyarncommon/RpcHelper.h"

#include "common/Exception.h"
#include "common/ExceptionInternal.h"

using namespace Yarn;

namespace libyarn {

ApplicationMasterProtocol::ApplicationMasterProtocol(const std::string & schedHost,
		const std::string & schedPort, const std::string & tokenService,
		const SessionConfig & c, const RpcAuth & a) :
		auth(a), client(RpcClient::getClient()), conf(c), protocol(
		APPLICATION_MASTER_VERSION, APPLICATION_MASTER_PROTOCOL,
		AMRM_TOKEN_KIND), server(tokenService, schedHost, schedPort) {
}

ApplicationMasterProtocol::~ApplicationMasterProtocol() {
}

void ApplicationMasterProtocol::invoke(const RpcCall & call) {
	try {
		channel = &client.getChannel(auth, protocol, server, conf);
		channel->invoke(call);
		channel->close(false);
	} catch (...) {
		channel->close(false);
		throw;
	}
}

RegisterApplicationMasterResponse ApplicationMasterProtocol::registerApplicationMaster(
		RegisterApplicationMasterRequest &request) {
	try {
		RegisterApplicationMasterResponseProto responseProto;
		RegisterApplicationMasterRequestProto requestProto = request.getProto();
		invoke(RpcCall(true, "registerApplicationMaster", &requestProto, &responseProto));
		return RegisterApplicationMasterResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	} catch (...) {
        THROW(YarnIOException,
              "Unexpected exception: when calling "
              "ApplicationMasterProtocol::registerApplicationMaster in %s: %d",
              __FILE__, __LINE__);
	}
}


AllocateResponse ApplicationMasterProtocol::allocate(AllocateRequest &request) {
	try {
		AllocateRequestProto requestProto = request.getProto();
		AllocateResponseProto responseProto;
		invoke(RpcCall(true, "allocate", &requestProto, &responseProto));
		return AllocateResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	}
	catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationMasterProtocol::allocate in %s: %d",
			  __FILE__, __LINE__);
	}
}

FinishApplicationMasterResponse ApplicationMasterProtocol::finishApplicationMaster(
		FinishApplicationMasterRequest &request) {
	try {
		FinishApplicationMasterRequestProto requestProto = request.getProto();
		FinishApplicationMasterResponseProto responseProto;
		invoke(RpcCall(true, "finishApplicationMaster", &requestProto, &responseProto));
		return FinishApplicationMasterResponse(responseProto);
	} catch (const YarnFailoverException & e) {
		 throw;
	} catch (const YarnRpcServerException & e) {
		UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
		unwrapper.unwrap(__FILE__, __LINE__);
	}	catch (...) {
		THROW(YarnIOException,
			  "Unexpected exception: when calling "
			  "ApplicationMasterProtocol::finishApplicationMaster in %s: %d",
			  __FILE__, __LINE__);
	}
}

}
