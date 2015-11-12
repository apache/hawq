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

#include "NodeReport.h"

namespace libyarn {

NodeReport::NodeReport() {
	reportProto = NodeReportProto::default_instance();
}

NodeReport::NodeReport(const NodeReportProto &proto) : reportProto(proto) {
}

NodeReport::~NodeReport() {
}

NodeReportProto& NodeReport::getProto() {
	return reportProto;
}

void NodeReport::setNodeId(NodeId &nodeId) {
	NodeIdProto *proto = new NodeIdProto();
	proto->CopyFrom(nodeId.getProto());
	reportProto.set_allocated_nodeid(proto);
}
NodeId NodeReport::getNodeId() {
	return NodeId(reportProto.nodeid());
}

void NodeReport::setHttpAddress(string &address) {
	reportProto.set_httpaddress(address);
}

string NodeReport::getHttpAddress() {
	return reportProto.httpaddress();
}

void NodeReport::setRackName(string &name) {
	reportProto.set_rackname(name);
}

string NodeReport::getRackName() {
	return reportProto.rackname();
}

void NodeReport::setUsedResource(Resource &res) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(res.getProto());
	reportProto.set_allocated_used(proto);
}

Resource NodeReport::getUsedResource() {
	return Resource(reportProto.used());
}

void NodeReport::setResourceCapablity(Resource &capability) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(capability.getProto());
	reportProto.set_allocated_capability(proto);
}

Resource NodeReport::getResourceCapability() {
	return Resource(reportProto.capability());
}

void NodeReport::setNumContainers(int32_t num) {
	reportProto.set_numcontainers(num);
}

int32_t NodeReport::getNumContainers() {
	return reportProto.numcontainers();
}

void NodeReport::setNodeState(NodeState state) {
	reportProto.set_node_state((NodeStateProto)state);
}

NodeState NodeReport::getNodeState() {
	return (NodeState)reportProto.node_state();
}

void NodeReport::setHealthReport(string &healthReport) {
	reportProto.set_health_report(healthReport);
}
string NodeReport::getHealthReport() {
	return reportProto.health_report();
}

void NodeReport::setLastHealthReportTime(int64_t time) {
	reportProto.set_last_health_report_time(time);
}
int64_t NodeReport::getLastHealthReportTime() {
	return reportProto.last_health_report_time();
}

} /* namespace libyarn */
