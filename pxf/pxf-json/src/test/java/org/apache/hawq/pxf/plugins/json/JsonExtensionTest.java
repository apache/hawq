package org.apache.hawq.pxf.plugins.json;

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
import org.junit.Before;
import org.junit.Test;

public class JsonExtensionTest extends PxfUnit {

	private static final String IDENTIFIER = JsonAccessor.IDENTIFIER_PARAM;
	private List<Pair<String, DataType>> columnDefs = null;
	private List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();
	private List<String> output = new ArrayList<String>();

	@Before
	public void before() {

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("created_at", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("id", DataType.BIGINT));
		columnDefs.add(new Pair<String, DataType>("text", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("user.screen_name", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("entities.hashtags[0]", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("coordinates.coordinates[0]", DataType.FLOAT8));
		columnDefs.add(new Pair<String, DataType>("coordinates.coordinates[1]", DataType.FLOAT8));

		output.clear();
		extraParams.clear();
	}

	@After
	public void cleanup() throws Exception {
		columnDefs.clear();
	}

	@Test
	public void testCompressedMultilineJsonFile() throws Exception {

		extraParams.add(new Pair<String, String>(IDENTIFIER, "created_at"));

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets.tar.gz"), output);
	}

	@Test
	public void testMaxRecordLength() throws Exception {

		// variable-size-objects.json contains 3 json objects but only 2 of them fit in the 27 byte length limitation

		extraParams.add(new Pair<String, String>(IDENTIFIER, "key666"));
		extraParams.add(new Pair<String, String>("MAXLENGTH", "27"));

		columnDefs.clear();
		columnDefs.add(new Pair<String, DataType>("key666", DataType.TEXT));

		output.add("small object1");
		// skip the large object2 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		output.add("small object3");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/variable-size-objects.json"), output);
	}

	@Test
	public void testDataTypes() throws Exception {

		// TDOO: The BYTEA type is not tested. The implementation (val.asText().getBytes()) returns an array reference
		// and it is not clear whether this is the desired behavior.
		//
		// For the time being avoid using BYTEA type!!!

		// This test also verifies that the order of the columns in the table definition agnostic to the order of the
		// json attributes.

		extraParams.add(new Pair<String, String>(IDENTIFIER, "bintType"));

		columnDefs.clear();

		columnDefs.add(new Pair<String, DataType>("text", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("varcharType", DataType.VARCHAR));
		columnDefs.add(new Pair<String, DataType>("bpcharType", DataType.BPCHAR));
		columnDefs.add(new Pair<String, DataType>("smallintType", DataType.SMALLINT));
		columnDefs.add(new Pair<String, DataType>("integerType", DataType.INTEGER));
		columnDefs.add(new Pair<String, DataType>("realType", DataType.REAL));
		columnDefs.add(new Pair<String, DataType>("float8Type", DataType.FLOAT8));
		// The DataType.BYTEA type is left out for further validation.
		columnDefs.add(new Pair<String, DataType>("booleanType", DataType.BOOLEAN));
		columnDefs.add(new Pair<String, DataType>("bintType", DataType.BIGINT));

		output.add(",varcharType,bpcharType,777,999,3.15,3.14,true,666");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/datatypes-test.json"), output);
	}

	@Test(expected = IllegalStateException.class)
	public void testMissingArrayJsonAttribute() throws Exception {

		extraParams.add(new Pair<String, String>(IDENTIFIER, "created_at"));

		columnDefs.clear();

		columnDefs.add(new Pair<String, DataType>("created_at", DataType.TEXT));
		// User is not an array! An attempt to access it should throw an exception!
		columnDefs.add(new Pair<String, DataType>("user[0]", DataType.TEXT));

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-with-missing-text-attribtute.json"), output);
	}

	@Test
	public void testMissingJsonAttribute() throws Exception {

		extraParams.add(new Pair<String, String>(IDENTIFIER, "created_at"));

		// Missing attributes are substituted by an empty field
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,,SpreadButter,tweetCongress,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-with-missing-text-attribtute.json"), output);
	}

	@Test
	public void testMalformedJsonObject() throws Exception {

		extraParams.add(new Pair<String, String>(IDENTIFIER, "created_at"));

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add(",,,,,,"); // Expected: malformed json records are transformed into empty rows
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-broken.json"), output);
	}

	@Test
	public void testSmallTweets() throws Exception {

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
		output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-small.json"), output);
	}

	@Test
	public void testTweetsWithNull() throws Exception {

		output.add("Fri Jun 07 22:45:02 +0000 2013,,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,,text2,patronusdeadly,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/null-tweets.json"), output);
	}

	@Test
	public void testSmallTweetsWithDelete() throws Exception {

		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-small-with-delete.json"), output);
	}

	@Test
	public void testWellFormedJson() throws Exception {

		extraParams.add(new Pair<String, String>(IDENTIFIER, "created_at"));

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-pp.json"), output);
	}

	@Test
	public void testWellFormedJsonWithDelete() throws Exception {

		extraParams.add(new Pair<String, String>(IDENTIFIER, "created_at"));

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
				+ "src/test/resources/tweets-pp-with-delete.json"), output);
	}

	@Test
	public void testMultipleFiles() throws Exception {

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
