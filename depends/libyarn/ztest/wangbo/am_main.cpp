/*
 * mytest.cpp
 *
 *  Created on: Jun 12, 2014
 *      Author: bwang
 */
#include <iostream>
#include <hdfs/ApplicationMaster.h>

#include <iostream>
#include "../build/src/YARN_yarn_service_protos.pb.h"
using namespace libyarn;
int main(void) {

	std::string host("localhost");
	std::string port("8099");
	ApplicationMaster *amTest = new ApplicationMaster(
			host, port);
	amTest->registerApplicationMaster("localhost", 8098, "");

	delete amTest;
	return 0;
}
