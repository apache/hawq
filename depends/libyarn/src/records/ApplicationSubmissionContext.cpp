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

#include "ApplicationSubmissionContext.h"

namespace libyarn {

ApplicationSubmissionContext::ApplicationSubmissionContext() {
	submitCtxProto = ApplicationSubmissionContextProto::default_instance();
}

ApplicationSubmissionContext::ApplicationSubmissionContext(
		const ApplicationSubmissionContextProto &proto) :
		submitCtxProto(proto) {
}

ApplicationSubmissionContext::~ApplicationSubmissionContext() {
}

ApplicationSubmissionContextProto& ApplicationSubmissionContext::getProto() {
	return submitCtxProto;
}

void ApplicationSubmissionContext::setApplicationId(ApplicationId &appId) {
	ApplicationIdProto *proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	submitCtxProto.set_allocated_application_id(proto);
}

ApplicationId ApplicationSubmissionContext::getApplicationId() {
	return ApplicationId(submitCtxProto.application_id());
}

void ApplicationSubmissionContext::setApplicationName(string &applicationName) {
	submitCtxProto.set_application_name(applicationName);
}

string ApplicationSubmissionContext::getApplicationName() {
	return submitCtxProto.application_name();
}

void ApplicationSubmissionContext::setQueue(string &queue) {
	submitCtxProto.set_queue(queue);
}

string ApplicationSubmissionContext::getQueue() {
	return submitCtxProto.queue();
}

void ApplicationSubmissionContext::setPriority(Priority &priority) {
	PriorityProto *proto = new PriorityProto();
	proto->CopyFrom(priority.getProto());
	submitCtxProto.set_allocated_priority(proto);
}

Priority ApplicationSubmissionContext::getPriority() {
	return Priority(submitCtxProto.priority());
}

void ApplicationSubmissionContext::setAMContainerSpec(ContainerLaunchContext &ctx) {
	ContainerLaunchContextProto *proto = new ContainerLaunchContextProto();
	proto->CopyFrom(ctx.getProto());
	submitCtxProto.set_allocated_am_container_spec(proto);
}

ContainerLaunchContext ApplicationSubmissionContext::getAMContainerSpec() {
	return ContainerLaunchContext(submitCtxProto.am_container_spec());
}

void ApplicationSubmissionContext::setCancelTokensWhenComplete(bool flag) {
	submitCtxProto.set_cancel_tokens_when_complete(flag);
}

bool ApplicationSubmissionContext::getCancelTokensWhenComplete() {
	return submitCtxProto.cancel_tokens_when_complete();
}

void ApplicationSubmissionContext::setUnmanagedAM(bool flag) {
	submitCtxProto.set_unmanaged_am(flag);
}

bool ApplicationSubmissionContext::getUnmanagedAM() {
	return submitCtxProto.unmanaged_am();
}

void ApplicationSubmissionContext::setMaxAppAttempts(int32_t max) {
	submitCtxProto.set_maxappattempts(max);
}

int32_t ApplicationSubmissionContext::getMaxAppAttempts() {
	return submitCtxProto.maxappattempts();
}

void ApplicationSubmissionContext::setResource(Resource &resource) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(resource.getProto());
	submitCtxProto.set_allocated_resource(proto);
}

Resource ApplicationSubmissionContext::getResource() {
	return Resource(submitCtxProto.resource());
}

void ApplicationSubmissionContext::setApplicationType(string &type) {
	submitCtxProto.set_applicationtype(type);
}

string ApplicationSubmissionContext::getApplicationType() {
	return submitCtxProto.applicationtype();
}

} /* namespace libyarn */
