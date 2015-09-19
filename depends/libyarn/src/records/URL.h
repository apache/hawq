/*
 * URL.h
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#ifndef URL_H_
#define URL_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

//message URLProto {
//  optional string scheme = 1;
//  optional string host = 2;
//  optional int32 port = 3;
//  optional string file = 4;
//  optional string userInfo = 5;
//}

class URL {
public:
	URL();
	URL(const URLProto &proto);
	virtual ~URL();

	URLProto& getProto();

	string getScheme();
	void setScheme(string &scheme);

	string getHost();
	void setHost(string &host);

	int32_t getPort();
	void setPort(int32_t port);

	string getFile();
	void setFile(string &file);

	string getUserInfo();
	void setUserInfo(string &userInfo);

private:
	URLProto urlProto;
};

} /* namespace libyarn */

#endif /* URL_H_ */
