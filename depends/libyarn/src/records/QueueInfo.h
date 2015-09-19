/*
 * QueueInfo.h
 *
 *  Created on: Jul 22, 2014
 *      Author: bwang
 */

#ifndef QUEUEINFO_H_
#define QUEUEINFO_H_

#include <list>

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

#include "QueueState.h"
#include "ApplicationReport.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message QueueInfoProto {
  optional string queueName = 1;
  optional float capacity = 2;
  optional float maximumCapacity = 3;
  optional float currentCapacity = 4;
  optional QueueStateProto state = 5;
  repeated QueueInfoProto childQueues = 6;
  repeated ApplicationReportProto applications = 7;
}
*/

class QueueInfo {
public:
	QueueInfo();
	QueueInfo(const QueueInfoProto &proto);
	virtual ~QueueInfo();

	QueueInfoProto& getProto();

	void setQueueName(string &queueName);
	string getQueueName();

	void setCapacity(float capacity);
	float getCapacity();

	void setMaximumCapacity(float maximumCapacity);
	float getMaximumCapacity();

	void setCurrentCapacity(float currentCapacity);
	float getCurrentCapacity();

	void setQueueState(QueueState queueState);
	QueueState getQueueState();

	void setChildQueues(list<QueueInfo> &childQueues);
	list<QueueInfo> getChildQueues();

	void setApplicationReports(list<ApplicationReport> &appReports);
	list<ApplicationReport> getApplicationReports();

private:
	QueueInfoProto infoProto;
};

} /* namespace libyarn */

#endif /* QUEUEINFO_H_ */
