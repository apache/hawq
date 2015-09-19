/*
 * GetContainersResponse.h
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
 */

#ifndef GETCONTAINERSRESPONSE_H_
#define GETCONTAINERSRESPONSE_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/ContainerReport.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

/*
message GetContainersResponseProto {
  repeated ContainerReportProto containers_reports = 1;
}
*/

class GetContainersResponse {
public:
	GetContainersResponse();
	GetContainersResponse(const GetContainersResponseProto &proto);
	virtual ~GetContainersResponse();

	GetContainersResponseProto& getProto();

	list<ContainerReport> getcontainersReportList();
	void setcontainersReportList(list<ContainerReport> &containersReport);

private:
	GetContainersResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETCONTAINERSRESPONSE_H_ */
