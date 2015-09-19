/*
 * ContainerReport.cpp
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
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




