package org.apache.pxf.hawq.plugins.json;

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.plugins.hdfs.HdfsDataFragmenter;
import org.apache.hawq.pxf.plugins.json.JsonAccessor;
import org.apache.hawq.pxf.plugins.json.JsonResolver;
import org.junit.After;
import org.junit.Test;

public class JsonExtensionTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	static {

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("created_at", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("id", DataType.BIGINT));
		columnDefs.add(new Pair<String, DataType>("text", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("user.screen_name", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("entities.hashtags[0]", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("coordinates.coordinates[0]", DataType.FLOAT8));
		columnDefs.add(new Pair<String, DataType>("coordinates.coordinates[1]", DataType.FLOAT8));
	}

	@After
	public void cleanup() throws Exception {
		extraParams.clear();
	}

	@Test
	public void testSmallTweets() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
		output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-small.json"), output);
	}

	@Test
	public void testTweetsWithNull() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,,text2,patronusdeadly,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/null-tweets.json"), output);
	}

	@Test
	public void testSmallTweetsWithDelete() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-small-with-delete.json"), output);
	}

	@Test
	public void testWellFormedJson() throws Exception {

		extraParams.add(new Pair<String, String>("IDENTIFIER", "record"));
		extraParams.add(new Pair<String, String>("ONERECORDPERLINE", "false"));

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-pp.json"), output);
	}

	@Test
	public void testWellFormedJsonWithDelete() throws Exception {

		extraParams.add(new Pair<String, String>("IDENTIFIER", "record"));
		extraParams.add(new Pair<String, String>("ONERECORDPERLINE", "false"));

		List<String> output = new ArrayList<String>();

		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-pp-with-delete.json"), output);
	}

	@Test
	public void testMultipleFiles() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
		output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");
		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertUnorderedOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-small*.json"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return HdfsDataFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return JsonAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return JsonResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}
}
