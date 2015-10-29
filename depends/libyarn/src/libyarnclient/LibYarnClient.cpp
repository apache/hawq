#include <sstream>
#include <list>
#include "rpc/RpcAuth.h"
#include "common/XmlConfig.h"
#include "common/SessionConfig.h"

#include "ApplicationClient.h"
#include "ApplicationMaster.h"
#include "ContainerManagement.h"

#include "LibYarnClient.h"
#include "LibYarnConstants.h"

#include "common/Logger.h"

using namespace Yarn::Internal;

namespace libyarn {

LibYarnClient::LibYarnClient(string &user, string &rmHost, string &rmPort,
		string &schedHost, string &schedPort, string &amHost,
		int32_t amPort, string &am_tracking_url,int heartbeatInterval) :
		amUser(user), schedHost(schedHost), schedPort(schedPort), amHost(amHost),
		amPort(amPort), am_tracking_url(am_tracking_url),
		heartbeatInterval(heartbeatInterval),clientJobId(""),response_id(0),
		keepRun(true){
        pthread_mutex_init( &(heartbeatLock), NULL );

		amrmClient = NULL;
		appClient = (void*) new ApplicationClient(user, rmHost, rmPort);
		nmClient = (void*) new ContainerManagement();
}
#ifdef MOCKTEST
LibYarnClient::LibYarnClient(string &rmHost, string &rmPort, string &schedHost,
		string &schedPort, string &amHost, int32_t amPort,
		string &am_tracking_url, int heartbeatInterval,Mock::TestLibYarnClientStub *stub):
		schedHost(schedHost), schedPort(schedPort), amHost(amHost),
		amPort(amPort), am_tracking_url(am_tracking_url),
		heartbeatInterval(heartbeatInterval),clientJobId(""),response_id(1),
		keepRun(false){
		pthread_mutex_init( &(heartbeatLock), NULL );
		libyarnStub = stub;
		appClient = (void*) libyarnStub->getApplicationClient();
		amrmClient = (void*) libyarnStub->getApplicationMaster();
		nmClient = (void*) libyarnStub->getContainerManagement();
}
#endif

LibYarnClient::~LibYarnClient() {
	if (amrmClient != NULL){
		delete (ApplicationMaster*)amrmClient;
	}
	delete (ApplicationClient*)appClient;
	delete (ContainerManagement*)nmClient;
}

string LibYarnClient::getErrorMessage() {
    return errorMessage;
}

void LibYarnClient::setErrorMessage(string errorMsg){
    errorMessage = errorMsg;
}

bool LibYarnClient::isJobHealthy() {
	return keepRun;
}

list<ResourceRequest> LibYarnClient::getAskRequests() {
	return askRequests;
}

void* heartbeatFunc(void* args) {
	int failcounter = 0;

	LibYarnClient *client = (LibYarnClient*)args;

	while (client->keepRun) {
		try {
			client->dummyAllocate();
			failcounter = 0;
		}
		catch(const YarnException &e) {
			LOG(WARNING, "LibYarnClient::heartbeatFunc, dummy allocation "
						 "is not correctly executed with exception raised. %s",
						 e.msg());
			failcounter++;
			if ( failcounter > 0 ) {
				// In case retry too many times with errors/exceptions, this
				// thread will return. LibYarn has to re-register application
				// and start the heartbeat thread again.
				LOG(WARNING, "LibYarnClient::heartbeatFunc, there are too many "
						     "failures raised. This heart-beat thread exits now.");
				client->keepRun = false;
			}
		}
		usleep((client->heartbeatInterval) * 1000);
	}

	LOG(INFO, "LibYarnClient::heartbeatFunc, goes into exit phase.");
	return (void *)0;
}

int LibYarnClient::createJob(string &jobName, string &queue,string &jobId) {
    try{
    	//Only one jobId for the client right now.
    	if (clientJobId != ""){
    		throw std::invalid_argument( "Exist an application for the client");
    	}
        ApplicationClient *applicationClient = (ApplicationClient*)appClient;

        //1. getNewApplication
        ApplicationID appId = applicationClient->getNewApplication();
        LOG(INFO, "LibYarnClient::createJob, getNewApplication finished, appId:[clusterTimeStamp:%lld,id:%d]",
                appId.getClusterTimestamp(), appId.getId());

        //2. submitApplication
        ApplicationSubmissionContext appSubmitCtx;
        appSubmitCtx.setApplicationId(appId);
        appSubmitCtx.setApplicationName(jobName);
        appSubmitCtx.setQueue(queue);

        Priority priority(0);
        appSubmitCtx.setPriority(priority);

        ContainerLaunchContext amContainerSpec;
        appSubmitCtx.setAMContainerSpec(amContainerSpec);

        appSubmitCtx.setUnmanagedAM(true);
        appSubmitCtx.setMaxAppAttempts(1);

        applicationClient->submitApplication(appSubmitCtx);
        LOG(INFO, "LibYarnClient::createJob, submitApplication finished");

        //3. wait util AM is ACCEPTED and return the AMRMToken
        ApplicationReport report;
        while (true) {
            report = applicationClient->getApplicationReport(appId);
            LOG(INFO,"LibYarnClient::createJob, appId[cluster_timestamp:%lld,id:%d], appState:%d",
                    appId.getClusterTimestamp(), appId.getId(), report.getYarnApplicationState());
            if ((report.getAMRMToken().getPassword() != "") && report.getYarnApplicationState() == YarnApplicationState::ACCEPTED) {
                break;
            } else {
                usleep(1000000L);
            }
        }

        clientAppId = appId;
        clientAppAttempId = report.getCurrentAppAttemptId();

        //4.1 new ApplicationMaster
        Token token = report.getAMRMToken();
        UserInfo user;
        if (applicationClient->getMethod() == SIMPLE)
			user = UserInfo::LocalUser();
		else if (applicationClient->getMethod() == KERBEROS) {
			user.setEffectiveUser(applicationClient->getPrincipal());
			user.setRealUser(applicationClient->getUser());
		} else {
			LOG(WARNING, "LibYarnClient::createJob: unsupported RPC method:%d. ", applicationClient->getMethod());
		}

		Yarn::Token AMToken;
		AMToken.setIdentifier(token.getIdentifier());
		AMToken.setKind(token.getKind());
		AMToken.setPassword(token.getPassword());
		AMToken.setService(token.getService());

        user.addToken(AMToken);
#ifndef MOCKTEST
        amrmClient = (void*) new ApplicationMaster(this->schedHost, this->schedPort,
                user, token.getService());
#endif

        //4.2 register to RM scheduler as AM
        ((ApplicationMaster*) amrmClient)->registerApplicationMaster(amHost, amPort,am_tracking_url);
        LOG(INFO, "LibYarnClient::createJob, registerApplicationMaster finished");

#ifndef MOCKTEST
        //5. setup the heartbeat thread to allocate, release, heartbeat
        int rc = pthread_create(&heartbeatThread, NULL, heartbeatFunc, this);
        if ( rc != 0 ) {
        	LOG(INFO, "LibYarnClient::createJob, fail to create heart-beat thread. "
        			  "error code %d", rc);
        	throw std::runtime_error( "Fail to create heart-beat thread.");
        }
#endif

        LOG(INFO,"LibYarnClient::createJob, after AM register to RM, a heartbeat thread has been started");
        //6. return jobId
        stringstream ss;
        ss << "job_" << appId.getClusterTimestamp() << "_" << appId.getId();
        jobId = ss.str();

    	LOG(INFO,"LibYarnClient::createJob, appId[cluster_timestamp:%lld,id:%d]",
    				clientAppId.getClusterTimestamp(), clientAppId.getId());
        clientJobId = jobId;
        return FR_SUCCEEDED;
    } catch (const YarnNetworkConnectException &e) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::createJob, catch network connection exception:" << e.what();
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
	} catch (const std::exception &e) {
        stringstream errorMsg;
        errorMsg << "LibYarnClient::createJob, catch exception:" << e.what();
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    } catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::createJob, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}

int LibYarnClient::forceKillJob(string &jobId) {

#ifndef MOCKTEST
    if ( keepRun ) {
        keepRun=false;
        void *thrc = NULL;
        int rc = pthread_join(heartbeatThread, &thrc);
        if ( rc != 0 ) {
            LOG(INFO, "LibYarnClient::forceKillJob, fail to join heart-beat thread. "
                      "error code %d", rc);
            return FR_FAILED;
        }
    }
#endif

    try{
        if (jobId != clientJobId) {
            throw std::invalid_argument("The jobId is wrong, please check the jobId argument");
        }

        for (map<int,Container*>::iterator it = jobIdContainers.begin(); it != jobIdContainers. end(); it++) {
            ostringstream key;
            Container *container = it->second;
            key << container->getNodeId().getHost() << ":" << container->getNodeId().getPort();
            Token nmToken = nmTokenCache[key.str()];
            ((ContainerManagement*)nmClient)->stopContainer((*container), nmToken);
            LOG(INFO,"LibYarnClient::forceKillJob, container:%d is stopped",container->getId().getId());
        }

        ((ApplicationClient*) appClient)->forceKillApplication(clientAppId);
        LOG(INFO, "LibYarnClient::forceKillJob, forceKillApplication");

        for (map<int,Container*>::iterator it = jobIdContainers.begin(); it != jobIdContainers.end(); it++) {
            LOG(INFO,"LibYarnClient::forceKillJob, container:%d in jobIdContainers is deleted",it->second->getId().getId());
            delete it->second;
            it->second = NULL;
        }
        jobIdContainers.clear();
        activeFailContainerIds.clear();
        return FR_SUCCEEDED;
    } catch(std::exception& e){
        stringstream errorMsg;
        errorMsg << "LibYarnClient::forceKillJob, catch the exception:" << e.what();
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    } catch (...) {
        stringstream errorMsg;
        errorMsg << "LibYarnClient::forceKillJob, catch unexpected exception.";
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    }
}


void LibYarnClient::dummyAllocate() {

    pthread_mutex_lock(&heartbeatLock);
	ApplicationMaster* amrmClientAlias = (ApplicationMaster*) amrmClient;

	//1) requestProto_blank
	list<ResourceRequest> asksBlank;
	//2) releasesBlank
	list<ContainerId> releasesBlank;
	//3) blacklistRequestBlank
	ResourceBlacklistRequest blacklistRequestBlank;
	//4) progress
	float progress = 0.5;
	int allocatedNum = 0;

	try {
		LOG(INFO, "LibYarnClient::dummyAllocate, do a AM-RM heartbeat with response_id:%d", response_id);
		AllocateResponse response = amrmClientAlias->allocate(asksBlank,
															  releasesBlank,
															  blacklistRequestBlank,
															  response_id,
															  progress);
		response_id = response.getResponseId();
		list<Container> allocatedContainers = response.getAllocatedContainers();
		allocatedNum = allocatedContainers.size();
		LOG(INFO,"LibYarnClient::dummyAllocate returned response_id :%d", response_id);
		if (allocatedNum > 0) {
			/*
			 * In rare case, client gets allocated containers in heartbeat,
			 * free them immediately.
			 */
			LOG(INFO, "LibYarnClient::dummyAllocate returned allocated size: %d, "
					  "free them immediately.", allocatedNum);
			list<ContainerId> releases;
			for (list<Container>::iterator it = allocatedContainers.begin();
				 it != allocatedContainers.end(); it++) {
				releases.push_back((*it).getId());
			}
			list<ResourceRequest> asksBlank;
			ResourceBlacklistRequest blacklistRequestBlank;
			response = amrmClientAlias->allocate(asksBlank, releases,
												 blacklistRequestBlank, response_id, progress);
			response_id = response.getResponseId();
		}
		pthread_mutex_unlock(&heartbeatLock);
	}
	catch (const YarnException &e) {
		LOG(WARNING, "LibYarnClient::dummyAllocate, dummy allocation "
					 "is not correctly executed with exception raised. %s",
					 e.msg());
		pthread_mutex_unlock(&heartbeatLock);
		throw;
	}
}

void LibYarnClient::addResourceRequest(Resource capability,
									  int32_t num_containers,
									  string host,
									  int32_t priority,
									  bool relax_locality)
{
	ResourceRequest *req = new ResourceRequest();
	req->setCapability(capability);
	req->setNumContainers(num_containers);
	Priority priorityProto;
	priorityProto.setPriority(priority);
	req->setPriority(priorityProto);
	req->setResourceName(host);
	req->setRelaxLocality(relax_locality);
	try {
		askRequests.push_back(*req);
		LOG(INFO, "LibYarnClient::put a request into ask list, "
				  "mem:%d, cpu:%d, priority:%d, resource name:%s, relax:%d, num_containers:%d",
				  capability.getMemory(), capability.getVirtualCores(), priority, host.c_str(),
				  relax_locality, num_containers);
	} catch (std::exception &e) {
		LOG(WARNING, "LibYarnClient::Fail to add a resource request "
					 "to ask list. %s ", e.what());
	}
}

/*
 * This function creates resource requests according to the requirement of the caller, then put these
 * requests into a list. The logic is a little similar to java client AMRMClient:addContainerRequest().
 * There are three level nodes in YARN: off-switch(aka ANY), rack, host.By now, only ANY-level and host-level
 * is supported.
 *
 * Parameters:
 * 		jobId: jobId
 * 		capability: the quota of the resource
 * 		count: the required number of the containers
 * 		preferred: node list, NULL means ANY host. If one node's rack name is NULL, a default rack name is set.
 * 		priority: priority
 */
int LibYarnClient::addContainerRequests(string &jobId, Resource &capability, int32_t num_containers,
									   list<LibYarnNodeInfo> &preferred, int32_t priority, bool relax_locality)
{
	if (jobId != clientJobId) {
		throw std::invalid_argument("The jobId is wrong, check the jobId argument");
	}

	list<ResourceRequest> ask = this->getAskRequests();
	map<string, int32_t> inferredRacks;

	try {
		for (list<LibYarnNodeInfo>::iterator iter = preferred.begin();
				iter != preferred.end(); iter++) {
			LOG(INFO, "LibYarnClient::addContainerRequests, "
					  "get a preferred host info, host:%s,rack:%s,container number:%d",
					  iter->getHost().c_str(), iter->getRack().c_str(), iter->getContainerNum());
			/* add a resource request for this node */
			addResourceRequest(capability, iter->getContainerNum(), iter->getHost(), priority, true);
			map<string, int32_t>:: iterator it = inferredRacks.find(iter->getRack());
			if (it != inferredRacks.end())
				it->second += iter->getContainerNum();
			else
				inferredRacks.insert(make_pair(iter->getRack(), iter->getContainerNum()));
		}

		/* add resource requests for racks*/
		for (map<string, int32_t>:: iterator it = inferredRacks.begin() ;
			 it != inferredRacks.end(); it++)
			addResourceRequest(capability, it->second, it->first, priority, relax_locality);

		/* add resource request for off-switch */
		addResourceRequest(capability, num_containers, YARN_HOST_ANY, priority, relax_locality);

		return FR_SUCCEEDED;
	} catch (std::exception &e) {
    	stringstream errorMsg;
    	errorMsg << "LibYarnClient::addContainerRequests catch std exception:" << e.what();
        setErrorMessage(errorMsg.str());
		return FR_FAILED;
	} catch (...) {
    	stringstream errorMsg;
    	errorMsg << "LibYarnClient::addContainerRequests catch unexpected exception.";
        setErrorMessage(errorMsg.str());
		return FR_FAILED;
	}
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
int LibYarnClient::allocateResources(string &jobId,
			list<string> &blackListAdditions,
			list<string> &blackListRemovals,
            list<Container> &allocatedContainers,
            int32_t num_containers) {
    try{
    	AllocateResponse response;
    	int retry = 5;
    	int allocatedNumOnce = 0;
    	int allocatedNumTotal = 0;

        pthread_mutex_lock(&heartbeatLock);
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong, check the jobId argument");
		}

        ApplicationMaster*    amrmClientAlias = (ApplicationMaster*) amrmClient;
        list<Container>       allocatedContainerCache;
        list<ContainerReport> preContainerReports;
        preContainerReports = ((ApplicationClient*) appClient)->getContainers(clientAppAttempId);

        list<ContainerId> releasesBlank;
        ResourceBlacklistRequest blacklistRequest;
        blacklistRequest.setBlacklistAdditions(blackListAdditions);
        blacklistRequest.setBlacklistRemovals(blackListRemovals);
        float progress = 0.5;
        list<ResourceRequest> ask = this->getAskRequests();

        LOG(INFO,"LibYarnClient::allocate, ask: container number:%d,", num_containers);

		while (retry > 0) {
			LOG(INFO,"LibYarnClient::allocate with response id : %d", response_id);
			AllocateResponse response = amrmClientAlias->allocate(ask, releasesBlank,
								blacklistRequest, response_id, progress);
			response_id = response.getResponseId();
			LOG(INFO,"LibYarnClient::allocate returned response id : %d", response_id);
			list<NMToken> nmTokens =  response.getNMTokens();
			for (list<NMToken>::iterator it = nmTokens.begin(); it != nmTokens.end(); it++) {
				std::ostringstream oss;
				oss << (*it).getNodeId().getHost() << ":" << (*it).getNodeId().getPort();
				nmTokenCache[oss.str()] = (*it).getToken();
			}
			ask.clear();
			list<Container> allocatedContainerOnce = response.getAllocatedContainers();
			allocatedNumOnce = allocatedContainerOnce.size();
			if (allocatedNumOnce <= 0) {
				LOG(WARNING, "LibYarnClient:: fail to allocate from YARN RM, try again");
				retry--;
				if(retry == 0) {
					/* If failed, just return to Resource Broker to handle*/
					pthread_mutex_unlock(&heartbeatLock);
					LOG(WARNING,"LibYarnClient:: fail to allocate from YARN RM after retry several times");
					return FR_SUCCEEDED;
				}
			} else {
				allocatedNumTotal += allocatedNumOnce;
				allocatedContainerCache.insert(allocatedContainerCache.end(), allocatedContainerOnce.begin(), allocatedContainerOnce.end());
				LOG(INFO, "LibYarnClient:: allocate %d containers from YARN RM", allocatedNumOnce);
				if (allocatedNumTotal >= num_containers) {
					LOG(INFO, "LibYarnClient:: allocate enough containers from YARN RM, "
							  "expected:%d, total:%d", num_containers, allocatedNumTotal);
					break;
				}

			}
			usleep(TimeInterval::ALLOCATE_INTERVAL_MS);
        }

		LOG(INFO,"LibYarnClient::allocate, ask: response_id:%d, allocated container number:%ld",
				 response_id, allocatedNumTotal);

		/* a workaround for allocate more container than request */
        list<ContainerId> releases;
        list<ContainerReport> afterContainerReports;
        afterContainerReports = ((ApplicationClient*) appClient)->getContainers(clientAppAttempId);
        for (list<ContainerReport>::iterator ait=afterContainerReports.begin();
        		ait!=afterContainerReports.end(); ait++){
        	bool foundInPre = false;
        	for (list<ContainerReport>::iterator pit=preContainerReports.begin();pit!=preContainerReports.end();pit++){
				if (pit->getId().getId() == ait->getId().getId()) {
					foundInPre = true;
					break;
				}
        	}
        	if (!foundInPre){
        		bool foundInNewAllocated = false;
        		for (list<Container>::iterator cit = allocatedContainerCache.begin();cit!=allocatedContainerCache.end();cit++){
        			if(cit->getId().getId() == ait->getId().getId()){
        				foundInNewAllocated = true;
        				break;
        			}
        		}
        		if (!foundInNewAllocated){
        			releases.push_back((*ait).getId());
        		}
        	}
        }

        int totalNeedRelease = allocatedContainerCache.size() - num_containers;
        LOG(INFO,"LibYarnClient::allocateResources, ask: finished: total_allocated_containers:%ld, total_need_release:%d",
                 allocatedContainerCache.size(), totalNeedRelease);
        if(totalNeedRelease > 0) {
			for (int i = 0; i < totalNeedRelease; i++) {
				list<Container>::iterator it = allocatedContainerCache.begin();
				releases.push_back((*it).getId());
				allocatedContainerCache.erase(it);
			}

			list<ResourceRequest> asksBlank;
			ResourceBlacklistRequest blacklistRequestBlank;
			response = amrmClientAlias->allocate(asksBlank, releases,
					blacklistRequestBlank, response_id, progress);
			response_id = response.getResponseId();
        }

        /* 3. store allocated containers */
        for(list<Container>::iterator it = allocatedContainerCache.begin();it != allocatedContainerCache.end();it++){
            Container *container = new Container((*it));
            int containerId = container->getId().getId();
            jobIdContainers[containerId] = container;
        }
        allocatedContainers = allocatedContainerCache;

        LOG(INFO,"LibYarnClient::allocateResources, put all allocated containers size:%ld",
                 allocatedContainerCache.size());

        pthread_mutex_unlock(&heartbeatLock);

        return FR_SUCCEEDED;
    } catch(std::exception &e) {

    	stringstream errorMsg;
    	errorMsg << "LibYarnClient::allocateResources, catch exception:" << e.what();
        setErrorMessage(errorMsg.str());

        pthread_mutex_unlock(&heartbeatLock);
        return FR_FAILED;
    } catch (...) {

    	stringstream errorMsg;
    	errorMsg << "LibYarnClient::allocateResources, catch unexpected exception.";
		setErrorMessage(errorMsg.str());

		pthread_mutex_unlock(&heartbeatLock);
		return FR_FAILED;
    }
}

int LibYarnClient::releaseResources(string &jobId,int releaseContainerIds[],int releaseContainerSize) {
    try{
        pthread_mutex_lock(&heartbeatLock);
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong,please check the jobId argument");
		}
        ApplicationMaster* amrmClientAlias = (ApplicationMaster*) amrmClient;
        //1) asksBlank
        list<ResourceRequest> asksBlank;
        //2) releases
        list<ContainerId> releases;
        for (int i = 0;i < releaseContainerSize;i++){
            int containerId = releaseContainerIds[i];
            map<int,Container*>::iterator it = jobIdContainers.find(containerId);
            if (it != jobIdContainers.end()) {
                releases.push_back((it->second)->getId());
            }
        }
        //3) blacklistRequestBlank
        ResourceBlacklistRequest blacklistRequestBlank;
        //4) progress
        float progress = 1.0;

        LOG(INFO, "LibYarnClient::releaseResource, release size:%d",releases.size());
        AllocateResponse response = amrmClientAlias->allocate(asksBlank, releases,
                blacklistRequestBlank, response_id, progress);
        response_id = response.getResponseId();
        //erase from the map jobIdContainers
        for (list<ContainerId>::iterator it = releases.begin();it != releases.end();it++){
            LOG(INFO, "LibYarnClient::releaseResource, released ContainerId:%d",it->getId());
            map<int,Container*>::iterator cit = jobIdContainers.find(it->getId());
            if (cit != jobIdContainers.end()){
                delete cit->second;
                cit->second = NULL;
                jobIdContainers.erase(it->getId());
            }
            //erase the element if in activeFailContainers
            set<int>::iterator sit = activeFailContainerIds.find(it->getId());
            if(sit != activeFailContainerIds.end()){
                LOG(INFO, "LibYarnClient::releaseResource, remove %d from  activeFailContainerIds",(*sit));
                activeFailContainerIds.erase(*sit);
            }
        }
        LOG(INFO, "LibYarnClient::releaseResources, release complete");
        pthread_mutex_unlock(&heartbeatLock);
        return FR_SUCCEEDED;
    } catch(std::exception &e) {
        stringstream errorMsg;
        errorMsg << "LibYarnClient::releaseResources, catch exception:" << e.what();
        setErrorMessage(errorMsg.str());
        pthread_mutex_unlock(&heartbeatLock);
        return FR_FAILED;
    } catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::releaseResources, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		pthread_mutex_unlock(&heartbeatLock);
		return FR_FAILED;
    }
}

/*
---------------------
message StartContainerRequestProto {
  optional ContainerLaunchContextProto container_launch_context = 1;
  optional hadoop.common.TokenProto container_token = 2;
}

message StartContainerResponseProto {
  repeated StringBytesMapProto services_meta_data = 1;
}
---------------------
rpc startContainers(StartContainersRequestProto) returns (StartContainersResponseProto);
---------------------
message StartContainersRequestProto {
  repeated StartContainerRequestProto start_container_request = 1;
}

message StartContainersResponseProto {
  repeated StringBytesMapProto services_meta_data = 1;
  repeated ContainerIdProto succeeded_requests = 2;
  repeated ContainerExceptionMapProto failed_requests = 3;
}
*/
int LibYarnClient::activeResources(string &jobId,int activeContainerIds[],int activeContainerSize) {
    try{
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong,please check the jobId argument");
		}
	    LOG(INFO, "LibYarnClient::activeResources, activeResources started");

        for (int i = 0; i < activeContainerSize; i++){
            int containerId = activeContainerIds[i];
            map<int,Container*>::iterator it = jobIdContainers.find(containerId);
            if (it != jobIdContainers.end()) {
                try{
                    Container *container = it->second;
                    ostringstream key;
                    key << container->getNodeId().getHost() << ":" << container->getNodeId().getPort();

                    Token nmToken = nmTokenCache[key.str()];
                    string cmd("sleep 10000000000");
                    list<string> cmds;
                    cmds.push_back(cmd);
                    ContainerLaunchContext ctx;
                    ctx.setCommand(cmds);
                    StartContainerRequest request;
                    request.setContainerLaunchCtx(ctx);
                    Token cToken = container->getContainerToken();
                    request.setContainerToken(cToken);
                    ((ContainerManagement*)nmClient)->startContainer((*container), request, nmToken);
                }catch(std::exception& e){
	                LOG(INFO, "LibYarnClient::activeResources, activeResources Failed Id:%d,exception:%s",containerId,e.what());
                    activeFailContainerIds.insert(containerId);
                }
            }
        }
        //using namespace Yarn::Internal;
	    LOG(INFO, "LibYarnClient::activeResources, activeResources finished");
        return FR_SUCCEEDED;
    } catch(std::exception& e){
        stringstream errorMsg;
        errorMsg << "LibYarnClient::activeResources, Catch the Exception:" << e.what();
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    } catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::activeResources, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}
int LibYarnClient::getActiveFailContainerIds(set<int> &activeFailIds){
    activeFailIds = activeFailContainerIds;
    return FR_SUCCEEDED;
}

int LibYarnClient::finishJob(string &jobId, FinalApplicationStatus finalStatus) {

#ifndef MOCKTEST
	if ( keepRun ) {
    	// No need to run heart-beat thread now.
    	keepRun=false;
    	void *thrc = NULL;
    	int rc = pthread_join(heartbeatThread, &thrc);
    	if ( rc != 0 ) {
			LOG(INFO, "LibYarnClient::finishJob, fail to join heart-beat thread. "
					  "error code %d", rc);
			return FR_FAILED;
    	}
	}
#endif

	try{
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong,please check the jobId argument");
		}
        //1. we should stop all containers related with this job
        //ContainerManagement cmgmt;
        for (map<int,Container*>::iterator it = jobIdContainers.begin(); it != jobIdContainers. end(); it++) {
            ostringstream key;
            Container *container = it->second;
            key << container->getNodeId().getHost() << ":" << container->getNodeId().getPort();
            Token nmToken = nmTokenCache[key.str()];
            ((ContainerManagement*)nmClient)->stopContainer((*container), nmToken);
            LOG(INFO,"LibYarnClient::finishJob, container:%d are stopped",container->getId().getId());
        }
        LOG(INFO,"LibYarnClient::finishJob, all containers for jobId:%s are stopped",jobId.c_str());
        //2. finish AM
        string diagnostics("");
        string tracking_url("");
        ((ApplicationMaster*) amrmClient)->finishApplicationMaster(diagnostics, tracking_url, finalStatus);
        LOG(INFO, "LibYarnClient::finishJob, finish AM for jobId:%s, finalStatus:%d", jobId.c_str(), finalStatus);
        //free the Container* memory
        for (map<int,Container*>::iterator it = jobIdContainers.begin(); it != jobIdContainers.end(); it++) {
        	LOG(INFO,"LibYarnClient::finishJob, container:%d in jobIdContainers are delete",it->second->getId().getId());
            delete it->second;
            it->second = NULL;
        }
        jobIdContainers.clear();
        activeFailContainerIds.clear();
        return FR_SUCCEEDED;
    }
    catch(std::exception& e){
        stringstream errorMsg;
        errorMsg << "LibYarnClient::finishJob, Catch the Exception:" << e.what();
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    } catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::finishJob, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}

int LibYarnClient::getApplicationReport(string &jobId,ApplicationReport &applicationReport){
	try {
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong,please check the jobId argument");
		}
		LOG(INFO,"LibYarnClient::getApplicationReport, appId[cluster_timestamp:%lld,id:%d]",
				clientAppId.getClusterTimestamp(), clientAppId.getId());
		applicationReport = ((ApplicationClient*) appClient)->getApplicationReport(clientAppId);
		LOG(INFO,"LibYarnClient::getApplicationReport, appId[cluster_timestamp:%lld,id:%d],getCurrentAppAttemptId:%d",
				applicationReport.getApplicationId().getClusterTimestamp(),
				applicationReport.getApplicationId().getId(),
				applicationReport.getCurrentAppAttemptId().getAttemptId());

		return FR_SUCCEEDED;
	} catch (std::exception& e) {
		stringstream errorMsg;
		errorMsg << "LibYarnClient::getApplicationReport, Catch the Exception:"
				<< e.what();
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
	} catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::getApplicationReport, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}

int LibYarnClient::getContainerReports(string &jobId,list<ContainerReport> &containerReports){
	try {
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong,please check the jobId argument");
		}
		LOG(INFO,"LibYarnClient::getContainerReports, appId[cluster_timestamp:%lld,id:%d]",
				clientAppId.getClusterTimestamp(), clientAppId.getId());
		containerReports = ((ApplicationClient*) appClient)->getContainers(clientAppAttempId);
		return FR_SUCCEEDED;
	} catch (std::exception& e) {
		stringstream errorMsg;
		errorMsg << "LibYarnClient::getContainerReports, Catch the Exception:" << e.what();
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
	} catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::getContainerReports, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}

int LibYarnClient::getContainerStatuses(string &jobId,int32_t containerIds[],int containerSize,
						list<ContainerStatus> &containerStatues){
	try {
		if (jobId != clientJobId) {
			throw std::invalid_argument("The jobId is wrong,please check the jobId argument");
		}

		for (int i = 0; i < containerSize; i++) {
			int containerId = containerIds[i];
			map<int, Container*>::iterator it = jobIdContainers.find(containerId);
			if (it != jobIdContainers.end()) {
				try {
					Container *container = it->second;
					ostringstream key;
					key << container->getNodeId().getHost() << ":"<< container->getNodeId().getPort();
					Token nmToken = nmTokenCache[key.str()];
					ContainerStatus containerStatus = ((ContainerManagement*) nmClient)->getContainerStatus((*container),  nmToken);
					// the response containerId will be 0 if the request containerId is not exist
					if (containerStatus.getContainerId().getId() != 0){
						containerStatues.push_back(containerStatus);
					}
				} catch (std::exception& e) {
					LOG(INFO,"LibYarnClient::getContainerStatuses, getContainerStatuses Failed Id:%d,exception:%s",containerId, e.what());
				}
			}
		}
		return FR_SUCCEEDED;
	} catch (std::exception& e) {
		stringstream errorMsg;
		errorMsg << "LibYarnClient::getContainerStatuses, Catch the Exception:" << e.what();
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
	} catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::getContainerStatuses, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}

int LibYarnClient::getQueueInfo(string &queue, bool includeApps,
		bool includeChildQueues, bool recursive,QueueInfo &queueInfo) {
    try{
        queueInfo = ((ApplicationClient*) appClient)->getQueueInfo(queue, includeApps,
                includeChildQueues, recursive);
        return FR_SUCCEEDED;
    }
    catch(std::exception& e){
        stringstream errorMsg;
        errorMsg << "LibYarnClient::getQueueInfo, Catch the Exception:" << e.what();
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    } catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::getQueueInfo, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}

int LibYarnClient::getClusterNodes(list<NodeState> &states,list<NodeReport> &nodeReports) {
    try{
        nodeReports =  ((ApplicationClient*) appClient)->getClusterNodes(states);
        return FR_SUCCEEDED;
    }
    catch(std::exception& e){
        stringstream errorMsg;
        errorMsg << "LibYarnClient::getClusterNodes, Catch the Exception:" << e.what();
        setErrorMessage(errorMsg.str());
        return FR_FAILED;
    } catch (...) {
    	stringstream errorMsg;
		errorMsg << "LibYarnClient::getClusterNodes, catch unexpected exception.";
		setErrorMessage(errorMsg.str());
		return FR_FAILED;
    }
}
}
