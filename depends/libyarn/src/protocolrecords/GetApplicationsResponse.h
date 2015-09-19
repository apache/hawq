/*
 * GetApplicationsResponse.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef GETAPPLICATIONSRESPONSE_H_
#define GETAPPLICATIONSRESPONSE_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/ApplicationReport.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

//message GetApplicationsResponseProto {
//  repeated ApplicationReportProto applications = 1;
//}

class GetApplicationsResponse {
public:
	GetApplicationsResponse();
	GetApplicationsResponse(const GetApplicationsResponseProto &proto);
	virtual ~GetApplicationsResponse();

	GetApplicationsResponseProto& getProto();

	list<ApplicationReport> getApplicationList();
	void setApplicationList(list<ApplicationReport> &applications);

private:
	GetApplicationsResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETAPPLICATIONSRESPONSE_H_ */
