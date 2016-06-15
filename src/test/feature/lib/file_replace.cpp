#include <iostream>
#include <fstream>

#include "file_replace.h"

using std::string;
using std::unordered_map;
using std::getline;
using std::endl;
using std::ifstream;
using std::ofstream;

namespace hawq {
namespace test {

string FileReplace::replaceAllOccurrences(
	string str,
	const string& src,
	const string& dst)
{
	size_t start_pos = 0;
	while ((start_pos = str.find(src, start_pos)) != string::npos)
	{
		str.replace(start_pos, src.length(), dst);
		start_pos += dst.length();
	}

	return str;
}

void FileReplace::replace(
	const string& file_src,
	const string& file_dst,
	const unordered_map<string, string>& strs_src_dst)
{
	ifstream fin(file_src);
	ofstream fout(file_dst);
	string line;

	while ( getline(fin, line) )
	{
		for(auto & mit : strs_src_dst)
		{
			line = replaceAllOccurrences(line, mit.first, mit.second);
		}

		fout << line << endl;
	}

	fin.close();
	fout.close();
}

} // namespace test
} // namespace hawq
