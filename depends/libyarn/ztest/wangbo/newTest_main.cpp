/*
 * newTest.cpp
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#include "libyarn/Tess.h"
#include <iostream>
using namespace libyarn;
int main(void) {
	std::string host("localhost");
	std::string port("8032");
	Tess* t = new Tess();
	t->run(host, port);
	return 0;
}
