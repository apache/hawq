/*
 * MockContainerManagement.h
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#ifndef MOCKCONTAINERMANAGEMENT_H_
#define MOCKCONTAINERMANAGEMENT_H_

#include "gmock/gmock.h"
#include "libyarnclient/ContainerManagement.h"

using namespace libyarn;
using namespace std;

namespace Mock{
class MockContainerManagement : public ContainerManagement {
public:
	~MockContainerManagement(){
	}
	MOCK_METHOD3(startContainer, StartContainerResponse (Container &container,StartContainerRequest &request, libyarn::Token &nmToken));
	MOCK_METHOD2(stopContainer, void (Container &container, libyarn::Token &nmToken));
	MOCK_METHOD2(getContainerStatus, ContainerStatus (Container &container, libyarn::Token &nmToken));
};
}




#endif /* MOCKCONTAINERMANAGEMENT_H_ */
