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

class DefaultConfig {
public:
    DefaultConfig() : conf(new Yarn::Config) {
        bool reportError = false;
        const char * env = getenv("LIBYARN_CONF");
        std::string confPath = env ? env : "";

        if (!confPath.empty()) {
            size_t pos = confPath.find_first_of('=');

            if (pos != confPath.npos) {
                confPath = confPath.c_str() + pos + 1;
            }

            reportError = true;
        } else {
            confPath = "yarn-client.xml";
        }

        init(confPath, reportError);
    }

    DefaultConfig(const char * path) : conf(new Yarn::Config) {
        assert(path != NULL && strlen(path) > 0);
        init(path, true);
    }

    Yarn::Internal::shared_ptr<Yarn::Config> getConfig() {
        return conf;
    }

private:
    void init(const std::string & confPath, bool reportError) {
        if (access(confPath.c_str(), R_OK)) {
            if (reportError) {
                LOG(LOG_ERROR,
                    "Environment variable LIBYARN_CONF is set but %s cannot be read",
                    confPath.c_str());
            } else {
                return;
            }
        }

        conf->update(confPath.c_str());
    }
private:
    Yarn::Internal::shared_ptr<Yarn::Config> conf;
};

class ApplicationClient {
public:
	ApplicationClient(string &user, string &host, string &port);

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

    const std::string & getUser() const {return ((ApplicationClientProtocol*)appClient)->getUser();};

    AuthMethod getMethod() const {return ((ApplicationClientProtocol*)appClient)->getMethod();};

    const std::string getPrincipal() const {return ((ApplicationClientProtocol*)appClient)->getPrincipal();};

private:
	void *appClient;
};

} /* namespace libyarn */

#endif /* APPLICTIONCLIENT_H_ */
