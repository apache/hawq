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

#include "ApplicationReport.h"

namespace libyarn {

ApplicationReport::ApplicationReport() {
	reportProto =  ApplicationReportProto::default_instance();
}

ApplicationReport::ApplicationReport(const ApplicationReportProto &proto) :
		reportProto(proto) {
}

ApplicationReport::~ApplicationReport() {
}

ApplicationReportProto& ApplicationReport::getProto() {
	return reportProto;
}

void ApplicationReport::setApplicationId(ApplicationId &appId) {
	ApplicationIdProto *proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	reportProto.set_allocated_applicationid(proto);
}

ApplicationId ApplicationReport::getApplicationId() {
	return ApplicationId(reportProto.applicationid());
}

void ApplicationReport::setUser(string &user) {
	reportProto.set_user(user);
}

string ApplicationReport::getUser() {
	return reportProto.user();
}

void ApplicationReport::setQueue(string &queue) {
	reportProto.set_queue(queue);
}

string ApplicationReport::getQueue() {
	return reportProto.queue();
}

void ApplicationReport::setName(string &name) {
	reportProto.set_name(name);
}

string ApplicationReport::getName() {
	return reportProto.name();
}

void ApplicationReport::setHost(string &host) {
	reportProto.set_host(host);
}

string ApplicationReport::getHost() {
	return reportProto.host();
}

void ApplicationReport::setRpcPort(int32_t port) {
	reportProto.set_rpc_port(port);
}

int32_t ApplicationReport::getRpcPort() {
	return reportProto.rpc_port();
}

void ApplicationReport::setClientToAMToken(Token &token) {
	TokenProto *proto = new TokenProto();
	proto->CopyFrom(token.getProto());
	reportProto.set_allocated_client_to_am_token(proto);
}

Token ApplicationReport::getClientToAMToken() {
	return Token(reportProto.client_to_am_token());
}

void ApplicationReport::setYarnApplicationState(YarnApplicationState state) {
	reportProto.set_yarn_application_state((YarnApplicationStateProto)state);
}

YarnApplicationState ApplicationReport::getYarnApplicationState() {
	return (YarnApplicationState)reportProto.yarn_application_state();
}

void ApplicationReport::setTrackingUrl(string &url) {
	reportProto.set_trackingurl(url);
}

string ApplicationReport::getTrackingUrl() {
	return reportProto.trackingurl();
}

void ApplicationReport::setDiagnostics(string &diagnostics) {
	reportProto.set_diagnostics(diagnostics);
}

string ApplicationReport::getDiagnostics() {
	return reportProto.diagnostics();
}

void ApplicationReport::setStartTime(int64_t time) {
	reportProto.set_starttime(time);
}

int64_t ApplicationReport::getStartTime() {
	return reportProto.starttime();
}

void ApplicationReport::setFinishTime(int64_t time) {
	reportProto.set_finishtime(time);
}

int64_t ApplicationReport::getFinishTime() {
	return reportProto.finishtime();
}

void ApplicationReport::setFinalApplicationStatus(FinalApplicationStatus status) {
	reportProto.set_final_application_status((FinalApplicationStatusProto)status);
}

FinalApplicationStatus ApplicationReport::getFinalApplicationStatus() {
	return (FinalApplicationStatus)reportProto.final_application_status();
}

void ApplicationReport::setAppResourceUsage(ApplicationResourceUsageReport &usage) {
	ApplicationResourceUsageReportProto *proto = new ApplicationResourceUsageReportProto();
	proto->CopyFrom(usage.getProto());
	reportProto.set_allocated_app_resource_usage(proto);
}
ApplicationResourceUsageReport ApplicationReport::getAppResourceUsage() {
	return ApplicationResourceUsageReport(reportProto.app_resource_usage());
}

void ApplicationReport::setOriginalTrackingUrl(string &url) {
	reportProto.set_originaltrackingurl(url);
}

string ApplicationReport::getOriginalTrackingUrl() {
	return reportProto.originaltrackingurl();
}

void ApplicationReport::setCurrentAppAttemptId(ApplicationAttemptId &attemptId) {
	ApplicationAttemptIdProto *proto = new ApplicationAttemptIdProto();
	proto->CopyFrom(attemptId.getProto());
	reportProto.set_allocated_currentapplicationattemptid(proto);
}

ApplicationAttemptId ApplicationReport::getCurrentAppAttemptId() {
	return ApplicationAttemptId(reportProto.currentapplicationattemptid());
}

void ApplicationReport::setProgress(float progress) {
	reportProto.set_progress(progress);
}

float ApplicationReport::getProgress() {
	return reportProto.progress();
}

void ApplicationReport::setApplicationType(string &type) {
	reportProto.set_applicationtype(type);
}

string ApplicationReport::getApplicationType() {
	return reportProto.applicationtype();
}

void ApplicationReport::setAMRMToken(Token &token) {
	TokenProto *proto = new TokenProto();
	proto->CopyFrom(token.getProto());
	reportProto.set_allocated_am_rm_token(proto);
}

Token ApplicationReport::getAMRMToken() {
	return Token(reportProto.am_rm_token());
}

} /* namespace libyarn */
