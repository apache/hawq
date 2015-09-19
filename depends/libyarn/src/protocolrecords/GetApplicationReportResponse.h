/*
 * GetApplicationReportResponse.h
 *
 *  Created on: Jul 8, 2014
 *      Author: bwang
 */

#ifndef GETAPPLICATIONREPORTRESPONSE_H_
#define GETAPPLICATIONREPORTRESPONSE_H_

#include "records/ApplicationReport.h"
#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {
/*
message GetApplicationReportResponseProto {
  optional ApplicationReportProto application_report = 1;
}
*/
class GetApplicationReportResponse {
public:
	GetApplicationReportResponse();
	GetApplicationReportResponse(const GetApplicationReportResponseProto &proto);
	virtual ~GetApplicationReportResponse();

	GetApplicationReportResponseProto& proto();

	void setApplicationReport(ApplicationReport &appReport);
	ApplicationReport getApplicationReport();

private:
	GetApplicationReportResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETAPPLICATIONREPORTRESPONSE_H_ */
