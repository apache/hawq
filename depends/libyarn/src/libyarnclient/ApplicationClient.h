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
#include "Thread.h"

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

class RMInfo {

public:
	RMInfo();

	RMInfo(string &rmHost, string &rmPort) : host(rmHost), port(rmPort){};

    const std::string & getHost() const {
        return host;
    }

    void setHost(const std::string & rmHost) {
        host = rmHost;
    }

    const std::string & getPort() const {
        return port;
    }

    void setPort(const std::string & rmPort) {
        port = rmPort;
    }

	static std::vector<RMInfo> getHARMInfo(const Yarn::Config & conf, const char* name);

private:
	std::string host;
	std::string port;
};

class ApplicationClient {
public:
    ApplicationClient(string &user, string &host, string &port);

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

    const std::string & getUser(){uint32_t old=0; return getActiveAppClientProto(old)->getUser();};

    const AuthMethod getMethod(){uint32_t old=0; return getActiveAppClientProto(old)->getMethod();};

    const std::string getPrincipal(){uint32_t old=0; return getActiveAppClientProto(old)->getPrincipal();};

private:
    std::shared_ptr<ApplicationClientProtocol> getActiveAppClientProto(uint32_t & oldValue);
    void failoverToNextAppClientProto(uint32_t oldValue);

private:
    bool enableRMHA;
    int maxRMHARetry;
    mutex mut;
    /**
     * Each ApplicationClientProto object stands for a connection to a standby resource manager.
     * If application client fail in connecting the active resource manager, it will try the
     * next one in the list.
     */
    std::vector<std::shared_ptr<ApplicationClientProtocol>> appClientProtos;
    uint32_t currentAppClientProto;
};

} /* namespace libyarn */

#endif /* APPLICTIONCLIENT_H_ */
