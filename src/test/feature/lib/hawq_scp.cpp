#include "command.h"
#include "sql_util.h"
#include "hawq_scp.h"

using std::string;

namespace hawq {
namespace test {

bool HAWQScp::copy(const string& host_list, const string& src_file, const string& dst_dir)
{
	Command cmd("hawq scp " + host_list + " " + src_file + " =:" + dst_dir);
	if (cmd.run().getResultStatus() == 0)
	{
		return true;
	}
	else
	{
		return false;
	}
}

} // namespace test
} // namespace hawq
