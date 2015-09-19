/*
 * ApplicationResourceUsageReport.cpp
 *
 *  Created on: Jul 9, 2014
 *      Author: bwang
 */

#include "ApplicationResourceUsageReport.h"

namespace libyarn {

ApplicationResourceUsageReport::ApplicationResourceUsageReport() {
	reportProto = ApplicationResourceUsageReportProto::default_instance();
}

ApplicationResourceUsageReport::ApplicationResourceUsageReport(
		const ApplicationResourceUsageReportProto &proto) :
		reportProto(proto) {
}

ApplicationResourceUsageReport::~ApplicationResourceUsageReport() {
}

ApplicationResourceUsageReportProto& ApplicationResourceUsageReport::getProto() {
	return reportProto;
}

void ApplicationResourceUsageReport::setNumUsedContainers(int32_t num) {
	reportProto.set_num_used_containers(num);
}

int32_t ApplicationResourceUsageReport::getNumUsedContainers() {
	return reportProto.num_used_containers();
}

void ApplicationResourceUsageReport::setNumReservedContainers(int32_t num) {
	reportProto.set_num_reserved_containers(num);
}

int32_t ApplicationResourceUsageReport::getNumReservedContainers() {
	return reportProto.num_reserved_containers();
}

void ApplicationResourceUsageReport::setUsedResources(Resource &resource) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(resource.getProto());
	reportProto.set_allocated_used_resources(proto);
}

Resource ApplicationResourceUsageReport::getUsedResources() {
	return Resource(reportProto.used_resources());
}

void ApplicationResourceUsageReport::setReservedResources(Resource &resource) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(resource.getProto());
	reportProto.set_allocated_reserved_resources(proto);
}

Resource ApplicationResourceUsageReport::getReservedResources() {
	return Resource(reportProto.reserved_resources());
}

void ApplicationResourceUsageReport::setNeededResources(Resource &resource) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(resource.getProto());
	reportProto.set_allocated_needed_resources(proto);
}

Resource ApplicationResourceUsageReport::getNeededResources() {
	return Resource(reportProto.needed_resources());
}



} /* namespace libyarn */
