/*
 * TestLibYarnClientStub.h
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#ifndef TESTLIBYARNCLIENTSTUB_H_
#define TESTLIBYARNCLIENTSTUB_H_

#include "libyarnclient/ApplicationClient.h"
#include "libyarnclient/ApplicationMaster.h"
#include "libyarnclient/ContainerManagement.h"

using namespace libyarn;
namespace Mock {
class TestLibYarnClientStub {
public:
	virtual ~TestLibYarnClientStub() {}

	virtual ApplicationClient* getApplicationClient() = 0;
	virtual ApplicationMaster* getApplicationMaster() = 0;
	virtual ContainerManagement* getContainerManagement() = 0;
};
}
#endif /* TESTLIBYARNCLIENTSTUB_H_ */
