#include <iostream>
#include <yarn/Test.h>

using namespace std;

int main(void)
{
	string rmHost("localhost");
	string rmPort("8032");
	string jobName("libyarn");
	string queue("default");
	string schedHost("localhost");
	string schedPort("8099");

	libyarn::Test t;
	t.run(jobName, queue, rmHost, rmPort, schedHost, schedPort);
	return 0;
}
