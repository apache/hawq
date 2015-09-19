#ifndef APPLICTIONCLIENT_H_
#define APPLICTIONCLIENT_H_

#include <list>

#include "libyarnserver/ApplicationClientProtocol.h"
#include "records/ApplicationID.h"
#include "records/ApplicationReport.h"
#include "records/ContainerReport.h"
#include "records/ApplicationSubmissionContext.h"
#include "records/YarnClusterMetrics.h"
#include "records/QueueUserACLInfo.h"

using namespace std;

namespace libyarn {

class ApplicationClient {
public:
	ApplicationClient(string &host, string &port);

	ApplicationClient(ApplicationClientProtocol *appclient);

	virtual ~ApplicationClient();

	virtual ApplicationID getNewApplication();

	virtual void submitApplication(ApplicationSubmissionContext &appContext);

	virtual ApplicationReport getApplicationReport(ApplicationID &appId);

	virtual list<ContainerReport> getContainers(ApplicationAttemptId &appAttempId);

	virtual list<NodeReport> getClusterNodes(list<NodeState> &state);

	virtual QueueInfo getQueueInfo(string &queue, bool includeApps,
			bool includeChildQueues, bool recursive);

	virtual void forceKillApplication(ApplicationID &appId);

	virtual YarnClusterMetrics getClusterMetrics();

	virtual list<ApplicationReport> getApplications(list<string> &applicationTypes,
			list<YarnApplicationState> &applicationStates);

	virtual list<QueueUserACLInfo> getQueueAclsInfo();

private:
	void *appClient;
};

} /* namespace libyarn */

#endif /* APPLICTIONCLIENT_H_ */
