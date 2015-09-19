/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#include "ResourceRequest.h"

namespace libyarn {

ResourceRequest::ResourceRequest() {
	requestProto = ResourceRequestProto::default_instance();
}

ResourceRequest::ResourceRequest(const ResourceRequestProto &proto) : requestProto(proto) {
}

ResourceRequest::~ResourceRequest() {
}

ResourceRequestProto& ResourceRequest::getProto(){
	return requestProto;
}

void ResourceRequest::setPriority(Priority &priority){
	PriorityProto* priorityProto = new PriorityProto();
	priorityProto->CopyFrom(priority.getProto());
	requestProto.set_allocated_priority(priorityProto);
}

Priority ResourceRequest::getPriority() {
	return Priority(requestProto.priority());
}

void ResourceRequest::setResourceName(string &name){
	requestProto.set_allocated_resource_name(new string(name));
}

string ResourceRequest::getResourceName() {
	return requestProto.resource_name();
}

void ResourceRequest::setCapability(Resource &capability) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(capability.getProto());
	requestProto.set_allocated_capability(proto);
}

Resource ResourceRequest::getCapability() {
	return Resource(requestProto.capability());
}

void ResourceRequest::setNumContainers(int32_t num) {
	requestProto.set_num_containers(num);
}

int32_t ResourceRequest::getNumContainers() {
	return requestProto.num_containers();
}

void ResourceRequest::setRelaxLocality(bool relaxLocality) {
	requestProto.set_relax_locality(relaxLocality);
}

bool ResourceRequest::getRelaxLocality() {
	return requestProto.relax_locality();
}

} /* namespace libyarn */
