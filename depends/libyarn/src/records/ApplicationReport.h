/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef APPLICATIONREPORT_H_
#define APPLICATIONREPORT_H_

//#include "client/Token.h"
#include "YARN_yarn_protos.pb.h"
#include "YARNSecurity.pb.h"


#include "Token.h"
#include "YarnApplicationState.h"
#include "ApplicationId.h"
#include "YarnApplicationState.h"
#include "FinalApplicationStatus.h"
#include "ApplicationResourceUsageReport.h"
#include "ApplicationAttemptId.h"

using std::string; 
using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {
/*
message ApplicationReportProto {
  optional ApplicationIdProto applicationId = 1;
  optional string user = 2;
  optional string queue = 3;
  optional string name = 4;
  optional string host = 5;
  optional int32 rpc_port = 6;
  optional hadoop.common.TokenProto client_to_am_token = 7;
  optional YarnApplicationStateProto yarn_application_state = 8;
  optional string trackingUrl = 9;
  optional string diagnostics = 10 [default = "N/A"];
  optional int64 startTime = 11;
  optional int64 finishTime = 12;
  optional FinalApplicationStatusProto final_application_status = 13;
  optional ApplicationResourceUsageReportProto app_resource_Usage = 14;
  optional string originalTrackingUrl = 15;
  optional ApplicationAttemptIdProto currentApplicationAttemptId = 16;
  optional float progress = 17;
  optional string applicationType = 18;
  optional hadoop.common.TokenProto am_rm_token = 19;
}
 */
class ApplicationReport {
public:
	ApplicationReport();
	ApplicationReport(const ApplicationReportProto &proto);
	virtual ~ApplicationReport();

	ApplicationReportProto& getProto();

	void setApplicationId(ApplicationId &appId);
	ApplicationId getApplicationId();

	void setUser(string &user);
	string getUser();

	void setQueue(string &queue);
	string getQueue();

	void setName(string &name);
	string getName();

	void setHost(string &host);
	string getHost();

	void setRpcPort(int32_t port);
	int32_t getRpcPort();

	void setClientToAMToken(Token &token);
	Token getClientToAMToken();

	void setYarnApplicationState(YarnApplicationState state);
	YarnApplicationState getYarnApplicationState();

	void setTrackingUrl(string &url);
	string getTrackingUrl();

	void setDiagnostics(string &diagnostics);
	string getDiagnostics();

	void setStartTime(int64_t time);
	int64_t getStartTime();

	void setFinishTime(int64_t time);
	int64_t getFinishTime();

	void setFinalApplicationStatus(FinalApplicationStatus status);
	FinalApplicationStatus getFinalApplicationStatus();

	void setAppResourceUsage(ApplicationResourceUsageReport &usage);
	ApplicationResourceUsageReport getAppResourceUsage();

	void setOriginalTrackingUrl(string &url);
	string getOriginalTrackingUrl();

	void setCurrentAppAttemptId(ApplicationAttemptId &attemptId);
	ApplicationAttemptId getCurrentAppAttemptId();

	void setProgress(float progress);
	float getProgress();

	void setApplicationType(string &type);
	string getApplicationType();

	void setAMRMToken(Token &token);
	Token getAMRMToken();

private:
	ApplicationReportProto reportProto;
};

} /* namespace libyarn */

#endif /* APPLICATIONREPORT_H_ */
