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

	GetApplicationReportResponseProto& getProto();

	void setApplicationReport(ApplicationReport &appReport);
	ApplicationReport getApplicationReport();

private:
	GetApplicationReportResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETAPPLICATIONREPORTRESPONSE_H_ */
