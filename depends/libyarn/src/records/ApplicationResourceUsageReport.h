/*
 * ApplicationResourceUsageReport.h
 *
 *  Created on: Jul 9, 2014
 *      Author: bwang
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
