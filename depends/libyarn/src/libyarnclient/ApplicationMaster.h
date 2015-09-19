/*
 * ApplicationMaster.h
 *
 *  Created on: Jun 19, 2014
 *      Author: bwang
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


using namespace std;
using namespace Yarn::Internal;
using namespace Yarn;

namespace libyarn {

class ApplicationMaster {
public:
	ApplicationMaster(string &schedHost, string &schedPort,
			UserInfo &user, const string &tokenService);

	ApplicationMaster(ApplicationMasterProtocol *rmclient);

	virtual ~ApplicationMaster();

	virtual RegisterApplicationMasterResponse registerApplicationMaster(string &amHost,
			int32_t amPort, string &am_tracking_url);

	virtual AllocateResponse allocate(list<ResourceRequest> &asks,
			list<ContainerId> &releases,
			ResourceBlacklistRequest &blacklistRequest, int32_t responseId,
			float progress);

	virtual bool finishApplicationMaster(string &diagnostics, string &trackingUrl,
			FinalApplicationStatus finalstatus);

private:
	void *rmClient;
};

} /* namespace libyarn */

#endif /* APPLICATIONMASTER_H_ */
