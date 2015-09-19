/*
 * MockApplicationMasterProtocol.h
 *
 *  Created on: Mar 6, 2015
 *      Author: weikui
 */

#ifndef MOCKAPPLICATIONMASTERPROTOCOL_H_
#define MOCKAPPLICATIONMASTERPROTOCOL_H_
#include "gmock/gmock.h"
#include "libyarnserver/ApplicationMasterProtocol.h"

using namespace libyarn;
using namespace std;

namespace Mock{
class MockApplicationMasterProtocol : public ApplicationMasterProtocol {
public:
	MockApplicationMasterProtocol(string & schedHost,
			string & schedPort, const string & tokenService,
			const SessionConfig & c, const RpcAuth & a):
			ApplicationMasterProtocol(schedHost,schedPort,tokenService, c,a){
	}
	~MockApplicationMasterProtocol(){
	}

	MOCK_METHOD1(registerApplicationMaster, RegisterApplicationMasterResponse(RegisterApplicationMasterRequest &request));
	MOCK_METHOD1(allocate, AllocateResponse(AllocateRequest &request));
	MOCK_METHOD1(finishApplicationMaster, FinishApplicationMasterResponse(FinishApplicationMasterRequest &request));

};
}
#endif /* MOCKAPPLICATIONMASTERPROTOCOL_H_ */
