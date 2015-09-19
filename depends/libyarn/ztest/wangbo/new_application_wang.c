#include "hadoop_rpc_utils.h"
#include "YARN_yarn_service_protos.pb-c.h"
#include "net_utils.h"
#include "str_utils.h"
#include "log_utils.h"
#include "hadoop_rpc_constants.h"
#include "application_client_protocol.h"
#include "application_master_protocol.h"
#include "YARN_yarn_protos.pb-c.h"
#include "libyarn_protocol.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>


void printResource(Resource* resource) {
	printf("id:%s\tcpu:%d\tmemory:%d\tstatus:%d\n", resource->id, resource->cpu,
			resource->memory, resource->status);
}

void printResourceList(ResourceGroup resourceGroup) {

	printf("Total Resource:%d\n (%s)\n", resourceGroup.resource_num, resourceGroup.groupId);
	for (int i = 0; i < resourceGroup.resource_num; ++i) {
		printResource(resourceGroup.resources[i]);
	}
}

int main(int argc, char** argv) {
    int rc;
    char* s="date";
    Hadoop__Yarn__StringLocalResourceMapProto* s1;
    Hadoop__Yarn__StringBytesMapProto* s2;
    Hadoop__Yarn__StringStringMapProto* s3;
    Hadoop__Yarn__ApplicationACLMapProto* s4;
    Hadoop__Yarn__NodeStateProto nodestateproto = HADOOP__YARN__NODE_STATE_PROTO__NS_RUNNING ;
    Hadoop__Yarn__NodeReportProto** nodereportproto;
    char* application_types;
    Hadoop__Yarn__YarnApplicationStateProto application_states = HADOOP__YARN__YARN_APPLICATION_STATE_PROTO__KILLED;
    Hadoop__Yarn__ApplicationReportProto **applications;

    // setup proxy
    hadoop_rpc_proxy_t* proxy = create_new_yarn_rpc_proxy(APPLICATION_CLIENT_PROTOCOL);
    if (!proxy) {
        printf("failed to create a new hadoop rpc proxy.\n");
        return -1;
    }

    if (0 != (rc = setup_connection(proxy, argv[1], atoi(argv[2])))) {
        printf("failed to connect to server.\n");
        return -1;
    }

    // send get new application request
    Hadoop__Yarn__GetNewApplicationResponseProto* response = getNewApplication(proxy);
    if (!response) {
        printf("failed to get new application from server.\n");
        return -1;
    }
    printf("got new application, app-id=%d\n", response->application_id->id);


/*  Hadoop__Yarn__ResourceProto resource = HADOOP__YARN__RESOURCE_PROTO__INIT;
    resource.memory = 350;
    resource.virtual_cores = 1;
*/
    Hadoop__Yarn__PriorityProto priority = HADOOP__YARN__PRIORITY_PROTO__INIT;
    priority.priority=1;

    Hadoop__Yarn__ContainerLaunchContextProto container = HADOOP__YARN__CONTAINER_LAUNCH_CONTEXT_PROTO__INIT;
/*
    Hadoop__Yarn__StringLocalResourceMapProto resourcemap = HADOOP__YARN__STRING_LOCAL_RESOURCE_MAP_PROTO__INIT;
    Hadoop__Yarn__StringBytesMapProto tytesmap = HADOOP__YARN__STRING_BYTES_MAP_PROTO__INIT;
    Hadoop__Yarn__StringStringMapProto stringmap = HADOOP__YARN__STRING_STRING_MAP_PROTO__INIT;
    Hadoop__Yarn__ApplicationACLMapProto aclmap = HADOOP__YARN__APPLICATION_ACLMAP_PROTO__INIT;
    container.command = &s;
    s1 = &resourcemap;
    s2 = &tytesmap;
    s3 = &stringmap;
    s4 = &aclmap;
    //container.n_localresources= 250;
    container.localresources = &s1;
    container.application_acls = &s4;
    container.service_data = &s2;
    container.environment = &s3;
*/
    Hadoop__Yarn__ApplicationSubmissionContextProto context = HADOOP__YARN__APPLICATION_SUBMISSION_CONTEXT_PROTO__INIT;
    context.application_id = response->application_id;
    context.priority= &priority;
    context.application_name="pivotal-bo";
    context.am_container_spec= &container;
    context.unmanaged_am = 1;
    context.has_unmanaged_am = 1;
    context.maxappattempts = 3;
    context.applicationtype = "YARN";
    context.queue = "default";
    context.cancel_tokens_when_complete = 1;


    Hadoop__Yarn__SubmitApplicationResponseProto* response3 = submitApplication(proxy,&context);
    if(!response3){
    	printf("failed to get new application from server.\n");
    	return -1;
    }
    printf("submission success!\n");

    Hadoop__Yarn__GetApplicationReportResponseProto* response4 = getApplicationReport(proxy, response->application_id);
    if(!response4){
    	printf("failed to get application report.\n");
    	return -1;
    }
    printf("name is %s\n",response4->application_report->name);
    printf("type is %s\n",response4->application_report->applicationtype);
    printf("queue is %s\n",response4->application_report->queue);
    printf("attempt id is %d\n",response4->application_report->currentapplicationattemptid->attemptid);

/*    sleep(6);
    Hadoop__Yarn__KillApplicationResponseProto* response5 = forceKillApplication(proxy, response->application_id);
    if(!response5){
    	printf("failed to get application report.\n");
    	return -1;
    }
    printf("kill application success!\n");
*/
    Hadoop__Yarn__GetClusterMetricsResponseProto* response6 = getClusterMetrics(proxy);
    if(!response6){
        printf("failed to get cluster metrics.\n");
        return -1;
    }
    printf("the number of node managers is %d\n",response6->cluster_metrics->num_node_managers);

    Hadoop__Yarn__GetClusterNodesResponseProto* response7 = getClusterNodes(proxy, &nodestateproto);
    if(!response7){
        printf("failed to get cluster nodes.\n");
        return -1;
    }
    printf("getclusternodes success!!!!\n");
    nodereportproto = response7->nodereports;
    printf("the address is %s\n", (* nodereportproto)->httpaddress);
    printf("the nodeid is:%s\n",(* nodereportproto)->nodeid->host);
    printf("the state is %d\n",(* nodereportproto)->node_state);

    application_types="YARN";
    Hadoop__Yarn__GetApplicationsResponseProto* response8 = getApplications(proxy, application_types, &application_states);
    applications = response8->applications;
    printf("final status is %d\n",(*applications)->final_application_status);
    printf("application state is %d\n",(*applications)->yarn_application_state);
    printf("user is %s, queue is %s, name is %s, host is %s, trackingurl is %s, diagnostics is %s, finish time is %lld\n",
    		(*applications)->user, (*applications)->name, (*applications)->queue, (*applications)->host, (*applications)->trackingurl,
    		(*applications)->diagnostics, (*applications)->finishtime);

    // setup proxy
    hadoop_rpc_proxy_t* proxy1 = create_new_yarn_rpc_proxy(APPLICATION_MASTER_PROTOCOL);
    if (!proxy) {
    	printf("failed to create a new hadoop rpc proxy.\n");
    	return -1;
    }

    if (0 != (rc = setup_connection(proxy1, argv[1], atoi(argv[3])))) {
    	printf("failed to connect to server.\n");
        return -1;
    }

    char* dummy="localhost";
    int dummy_i=8099;
    printf("11111\n");
    Hadoop__Yarn__RegisterApplicationMasterResponseProto* response9 = registerApplicationMaster(proxy1, dummy, dummy_i, dummy);
    printf("virtual cores is %d\n", response9->maximumcapability->virtual_cores);
    printf("memory is %d\n", response9->maximumcapability->memory);

    return 0;
}
