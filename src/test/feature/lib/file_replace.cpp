/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
