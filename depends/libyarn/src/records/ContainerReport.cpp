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
	reportProto.set_allocated_id(proto);
}

ContainerId ContainerReport::getId(){
	return ContainerId(reportProto.id());
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
	reportProto.set_allocated_nodeid(proto);
}
NodeId ContainerReport::getNodeId(){
	return NodeId(reportProto.nodeid());
}

void ContainerReport::setPriority(Priority &priority){
	PriorityProto *proto = new PriorityProto();
	proto->CopyFrom(priority.getProto());
	reportProto.set_allocated_priority(proto);
}

Priority ContainerReport::getPriority(){
	return Priority(reportProto.priority());
}

void ContainerReport::setStartTime(int64_t time){
	reportProto.set_starttime(time);
}

int64_t ContainerReport::getStartTime(){
	return reportProto.starttime();
}

void ContainerReport::setFinishTime(int64_t time){
	reportProto.set_finishtime(time);
}
int64_t ContainerReport::getFinishTime(){
	return reportProto.finishtime();
}

void ContainerReport::setContainerExitStatus(ContainerExitStatus container_exit_status){
	reportProto.set_container_exit_status((ContainerExitStatusProto)container_exit_status);
}

ContainerExitStatus ContainerReport::getContainerExitStatus(){
	return (ContainerExitStatus)reportProto.container_exit_status();
}

void ContainerReport::setContaierState(ContainerState state){
	reportProto.set_state((ContainerStateProto)state);
}

ContainerState ContainerReport::getContainerState(){
	return (ContainerState)reportProto.state();
}

void ContainerReport::setDiagnostics(string &diagnostics){
	reportProto.set_diagnostics(diagnostics);
}

string ContainerReport::getDiagnostics(){
	return reportProto.diagnostics();
}

void ContainerReport::setLogUrl(string &url){
	reportProto.set_logurl(url);
}

string ContainerReport::getLogUrl(){
	return reportProto.logurl();
}

}




