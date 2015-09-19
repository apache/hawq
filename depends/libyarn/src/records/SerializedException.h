/*
 * SerializedException.h
 *
 *  Created on: Jul 31, 2014
 *      Author: bwang
 */

#ifndef SERIALIZEDEXCEPTION_H_
#define SERIALIZEDEXCEPTION_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

//message SerializedExceptionProto {
//  optional string message = 1;
//  optional string trace = 2;
//  optional string class_name = 3;
//  optional SerializedExceptionProto cause = 4;
//}

class SerializedException {
public:
	SerializedException();
	SerializedException(const SerializedExceptionProto &proto);
	virtual ~SerializedException();

	SerializedExceptionProto& getProto();

	void setMessage(string &message);
	string getMessage();

	void setTrace(string &trace);
	string getTrace();

	void setClassName(string &name);
	string getClassName();

	void setCause(SerializedException &cause);
	SerializedException getCause();

private:
	SerializedExceptionProto exceptionProto;
};

} /* namespace libyarn */

#endif /* SERIALIZEDEXCEPTION_H_ */
