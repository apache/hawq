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

#include "ContainerReport.h"

namespace libyarn {
ContainerReport::ContainerReport(){
	reportProto =  ContainerReportProto::default_instance();
}

ContainerReport::ContainerReport(const ContainerReportProto &proto):
	reportProto(proto){
}

ContainerReport::~ContainerReport(){
}

ContainerReportProto& ContainerReport::getProto(){
	return reportProto;
}

void ContainerReport::setId(ContainerId &id){
	ContainerIdProto *proto = new ContainerIdProto();
	proto->CopyFrom(id.getProto());
	reportProto.set_allocated_container_id(proto);
}

ContainerId ContainerReport::getId(){
	return ContainerId(reportProto.container_id());
}

void ContainerReport::setResource(Resource &resource){
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(resource.getProto());
	reportProto.set_allocated_resource(proto);
}

Resource ContainerReport::getResource(){
	return Resource(reportProto.resource());
}

void ContainerReport::setNodeId(NodeId &nodeId){
	NodeIdProto *proto = new NodeIdProto();
	proto->CopyFrom(nodeId.getProto());
	reportProto.set_allocated_node_id(proto);
}
NodeId ContainerReport::getNodeId(){
	return NodeId(reportProto.node_id());
}

void ContainerReport::setPriority(Priority &priority){
	PriorityProto *proto = new PriorityProto();
	proto->CopyFrom(priority.getProto());
	reportProto.set_allocated_priority(proto);
}

Priority ContainerReport::getPriority(){
	return Priority(reportProto.priority());
}

void ContainerReport::setCreationTime(int64_t time){
	reportProto.set_creation_time(time);
}

int64_t ContainerReport::getCreationTime(){
	return reportProto.creation_time();
}

void ContainerReport::setFinishTime(int64_t time){
	reportProto.set_finish_time(time);
}
int64_t ContainerReport::getFinishTime(){
	return reportProto.finish_time();
}

void ContainerReport::setContainerExitStatus(ContainerExitStatus container_exit_status){
	reportProto.set_container_exit_status((ContainerExitStatusProto)container_exit_status);
}

ContainerExitStatus ContainerReport::getContainerExitStatus(){
	return (ContainerExitStatus)reportProto.container_exit_status();
}

void ContainerReport::setContainerState(ContainerState state){
	reportProto.set_container_state((ContainerStateProto)state);
}

ContainerState ContainerReport::getContainerState(){
	return (ContainerState)reportProto.container_state();
}

void ContainerReport::setDiagnostics(string &diagnostics){
	reportProto.set_diagnostics_info(diagnostics);
}

string ContainerReport::getDiagnostics(){
	return reportProto.diagnostics_info();
}

void ContainerReport::setLogUrl(string &url){
	reportProto.set_log_url(url);
}

string ContainerReport::getLogUrl(){
	return reportProto.log_url();
}

}




