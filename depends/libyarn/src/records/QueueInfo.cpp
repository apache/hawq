/*
 * QueueInfo.cpp
 *
 *  Created on: Jul 22, 2014
 *      Author: bwang
 */

#include "QueueInfo.h"

namespace libyarn {

QueueInfo::QueueInfo() {
	infoProto = QueueInfoProto::default_instance();
}

QueueInfo::QueueInfo(const QueueInfoProto &proto) : infoProto(proto) {
}

QueueInfo::~QueueInfo() {
}

QueueInfoProto& QueueInfo::getProto() {
	return infoProto;
}

void QueueInfo::setQueueName(string &queueName) {
	infoProto.set_queuename(queueName);
}

string QueueInfo::getQueueName() {
	return infoProto.queuename();
}

void QueueInfo::setCapacity(float capacity) {
	infoProto.set_capacity(capacity);
}

float QueueInfo::getCapacity() {
	return infoProto.capacity();
}

void QueueInfo::setMaximumCapacity(float maximumCapacity) {
	infoProto.set_maximumcapacity(maximumCapacity);
}

float QueueInfo::getMaximumCapacity() {
	return infoProto.maximumcapacity();
}

void QueueInfo::setCurrentCapacity(float currentCapacity) {
	infoProto.set_currentcapacity(currentCapacity);
}

float QueueInfo::getCurrentCapacity() {
	return infoProto.currentcapacity();
}

void QueueInfo::setQueueState(QueueState queueState) {
	infoProto.set_state((QueueStateProto) queueState);
}

QueueState QueueInfo::getQueueState() {
	return (QueueState) infoProto.state();
}

void QueueInfo::setChildQueues(list<QueueInfo> &childQueues) {
	list<QueueInfo>::iterator it = childQueues.begin();
	for (; it != childQueues.end(); it++) {
		QueueInfoProto* iProto = infoProto.add_childqueues();
		iProto->CopyFrom((*it).getProto());
	}

}

list<QueueInfo> QueueInfo::getChildQueues() {
	list<QueueInfo> queueInfo;
	for (int i = 0; i < infoProto.childqueues_size(); i++) {
		queueInfo.push_back(QueueInfo(infoProto.childqueues(i)));
	}
	return queueInfo;
}

void QueueInfo::setApplicationReports(list<ApplicationReport> &appReports) {
	list<ApplicationReport>::iterator it = appReports.begin();
	for (; it != appReports.end(); it++) {
		ApplicationReportProto *reprortProto = infoProto.add_applications();
		reprortProto->CopyFrom((*it).getProto());
	}
}

list<ApplicationReport> QueueInfo::getApplicationReports() {
	list<ApplicationReport> reports;
	for (int i = 0; i < infoProto.applications_size(); i++) {
		reports.push_back(ApplicationReport(infoProto.applications(i)));
	}
	return reports;
}

} /* namespace libyarn */
