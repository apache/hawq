/*
 * TestContainerManagementStub.h
 *
 *  Created on: Mar 9, 2015
 *      Author: weikui
 */

#ifndef TESTCONTAINERMANAGEMENTSTUB_H_
#define TESTCONTAINERMANAGEMENTSTUB_H_
#include "libyarnserver/ContainerManagementProtocol.h"

using namespace libyarn;
namespace Mock {
class TestContainerManagementStub {
public:
	virtual ~TestContainerManagementStub() {}

	virtual ContainerManagementProtocol* getContainerManagementProtocol() = 0;

};
}

#endif /* TESTCONTAINERMANAGEMENTSTUB_H_ */
