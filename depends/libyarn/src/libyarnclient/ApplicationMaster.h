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
