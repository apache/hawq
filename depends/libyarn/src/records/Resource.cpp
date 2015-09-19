/*
 * Resource.cpp
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#include "Resource.h"

namespace libyarn {

Resource::Resource(){
	resourceProto = ResourceProto::default_instance();
}

Resource::Resource(const ResourceProto &proto) : resourceProto(proto) {
}

Resource::~Resource() {
}

ResourceProto& Resource::getProto(){
	return resourceProto;
}

void Resource::setMemory(int memory){
	resourceProto.set_memory(memory);
}

int Resource::getMemory() {
	return resourceProto.memory();
}

void Resource::setVirtualCores(int virtualCores){
	resourceProto.set_virtual_cores(virtualCores);
}

int Resource::getVirtualCores() {
	return resourceProto.virtual_cores();
}

} /* namespace libyarn */
