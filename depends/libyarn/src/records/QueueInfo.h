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

#ifndef QUEUEINFO_H_
#define QUEUEINFO_H_

#include <list>

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

#include "QueueState.h"
#include "ApplicationReport.h"

using std::string; using std::list;
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
