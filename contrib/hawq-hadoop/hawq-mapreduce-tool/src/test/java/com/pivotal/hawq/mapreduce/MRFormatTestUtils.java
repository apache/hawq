package com.pivotal.hawq.mapreduce;

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


import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.pivotal.hawq.mapreduce.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;
import com.pivotal.hawq.mapreduce.metadata.MetadataAccessor;
import com.pivotal.hawq.mapreduce.util.HAWQJdbcUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.regex.Pattern;

import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_DB_URL;
import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_FOLDER;

/**
 * A collection of utility functions for all HAWQInputFormat tests.
 */
public final class MRFormatTestUtils {

	public static void runSQLs(Connection conn, String sqls) throws SQLException {
		Statement stmt = null;
		try {
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			stmt.execute(sqls);
			conn.commit();
		} catch (SQLException e) {
			conn.rollback();
			throw e;
		} finally {
			HAWQJdbcUtils.free(stmt, null);
		}
	}

	public static void runShellCommand(String command)
			throws IOException, InterruptedException {

		System.out.println("execute: " + command);
		Process process = Runtime.getRuntime().exec(command);
		process.waitFor();
	}

	/**
	 * Run mapreduce job using HAWQInputFormat locally.
	 * @param metadataFile table's metadata file
	 * @param outputFolder output folder
	 * @param mapperClass the mapper class to use
	 * @return exit code. 0 on success.
	 * @throws Exception
	 */
	public static int runMapReduceLocally(Path metadataFile, Path outputFolder,
										  Class<? extends Mapper> mapperClass) throws Exception {

		MapReduceLocalDriver driver = new MapReduceLocalDriver();

		if (mapperClass == null)
			return driver.run(new String[] {
				metadataFile.toString(), outputFolder.toString()});
		else
			return driver.run(new String[] {
					metadataFile.toString(), outputFolder.toString(), mapperClass.getName()
			});
	}

	/**
	 * Run mapreduce job using HAWQInputFormat in cluster mode.
	 * @param tableName table to read
	 * @param outputFolder output folder
	 * @param mapperClass the mapper class to use
	 * @return exit code. 0 on success.
	 * @throws Exception
	 */
	public static int runMapReduceOnCluster(String tableName, Path outputFolder,
											Class<? extends Mapper> mapperClass) throws Exception {

		MapReduceClusterDriver driver = new MapReduceClusterDriver();

		if (mapperClass == null)
			return driver.run(new String[] {
					tableName, TEST_DB_URL, outputFolder.toString()
			});
		else
			return driver.run(new String[] {
					tableName, TEST_DB_URL, outputFolder.toString(), mapperClass.getName()
			});
	}

	// Get column value as String. Note that ResultSet.getString() will return
	// value's string representation in DB format, which is different from the
	// format used in Java's toString method. E.g., DB use 't' and 'f' for bool,
	// while Java uses 'true' and 'false'. This will lead to problems when comparing
	// results from DB and InputFormat, therefore we use Java's toString representation
	// below for types like bool, date, etc.
	private static String getColumnValue(ResultSet rs, ResultSetMetaData rsmd, int colIndex)
			throws SQLException {

		String s = rs.getString(colIndex);
		if (rs.wasNull())
			return "null";

		switch (rsmd.getColumnType(colIndex)) {
			case Types.BIT: // bool or bit(n)
				if (s.equals("t") || s.equals("f"))
					return String.valueOf(rs.getBoolean(colIndex));
				else
					return s;
			case Types.DATE: // date
				return rs.getDate(colIndex).toString();
			case Types.TIME: // time or timetz
				return rs.getTime(colIndex).toString();
			case Types.TIMESTAMP: // timestamp or timestamptz
				return rs.getTimestamp(colIndex).toString();
			default:
				return s;
		}
	}

	/**
	 * Dump table's content into a list of rows, each row consists of
	 * column values separated by '|'.
	 *
	 * @param conn      Database connection
	 * @param tableName name of table to dump
	 * @return all rows of specified table
	 * @throws SQLException
	 */
	public static List<String> dumpTable(Connection conn, String tableName) throws SQLException {
		List<String> rows = Lists.newArrayList();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM " + tableName);

			ResultSetMetaData rsmd = rs.getMetaData();
			final int colnum = rsmd.getColumnCount();

			while (rs.next()) {
				StringBuilder buf = new StringBuilder();
				for (int i = 1; i < colnum; i++) {
					buf.append(getColumnValue(rs, rsmd, i)).append("|");
				}
				buf.append(getColumnValue(rs, rsmd, colnum));
				rows.add(buf.toString());
			}

		} finally {
			HAWQJdbcUtils.free(stmt, rs);
		}
		return rows;
	}

	public static void copyDataFilesToLocal(File metadataFile)
			throws IOException, InterruptedException {

		MetadataAccessor accessor = MetadataAccessor.newInstanceUsingFile(metadataFile.getPath());
		List<String> dataFiles = Lists.newArrayList();

		// get data files
		switch (accessor.getTableFormat()) {
			case AO:
				for (HAWQAOFileStatus fileStatus : accessor.getAOMetadata().getFileStatuses()) {
					dataFiles.add(fileStatus.getFilePath());
				}
				break;
			case Parquet:
				for (HAWQFileStatus fileStatus : accessor.getParquetMetadata().getFileStatuses()) {
					dataFiles.add(fileStatus.getFilePath());
				}
				break;
			default:
				throw new AssertionError("invalid table format " + accessor.getTableFormat());
		}

		// copy data files to local file system
		for (String from : dataFiles) {
			assert from.startsWith("/");
			File to = new File(TEST_FOLDER, from.substring(1));
			Files.createParentDirs(to);

			runShellCommand(String.format("hadoop fs -copyToLocal %s %s",
										  from, to.getPath()));
		}
	}

	private static final Pattern PATH_PATTERN = Pattern.compile("(- path:\\s*)(\\S*)");

	/**
	 * Replace all path locations in metadata file to use local path.
	 * @param metadataFile the file to transform
	 * @throws IOException
	 */
	public static void transformMetadata(File metadataFile) throws IOException {
		String content = Files.toString(metadataFile, Charsets.UTF_8);
		// replace all lines like
		// - path: /hawq-data/gpseg0/16385/119737/134935.1
		// to
		// - path: test-data/hawq-data/gpseg0/16385/119737/134935.1
		content = PATH_PATTERN.matcher(content).replaceAll("$1" + TEST_FOLDER + "$2");
		Files.write(content.getBytes(), metadataFile);
	}

	// Get a connection to the test database.
	public static Connection getTestDBConnection() throws SQLException {
		return HAWQJdbcUtils.getConnection("jdbc:postgresql://" + TEST_DB_URL, null, null);
	}
}
