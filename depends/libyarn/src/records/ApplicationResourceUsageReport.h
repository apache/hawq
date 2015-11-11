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

#ifndef APPLICATIONRESOURCEUSAGEREPORT_H_
#define APPLICATIONRESOURCEUSAGEREPORT_H_

#include "YARN_yarn_protos.pb.h"
#include "Resource.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message ApplicationResourceUsageReportProto {
  optional int32 num_used_containers = 1;
  optional int32 num_reserved_containers = 2;
  optional ResourceProto used_resources = 3;
  optional ResourceProto reserved_resources = 4;
  optional ResourceProto needed_resources = 5;
}
 */

class ApplicationResourceUsageReport {
public:
	ApplicationResourceUsageReport();
	ApplicationResourceUsageReport(const ApplicationResourceUsageReportProto &proto);
	virtual ~ApplicationResourceUsageReport();

	ApplicationResourceUsageReportProto& getProto();

	void setNumUsedContainers(int32_t num);
	int32_t getNumUsedContainers();

	void setNumReservedContainers(int32_t num);
	int32_t getNumReservedContainers();

	void setUsedResources(Resource &resource);
	Resource getUsedResources();

	void setReservedResources(Resource &resource);
	Resource getReservedResources();

	void setNeededResources(Resource &resource);
	Resource getNeededResources();

private:
	ApplicationResourceUsageReportProto reportProto;

};

} /* namespace libyarn */

#endif /* APPLICATIONRESOURCEUSAGEREPORT_H_ */
