package com.pivotal.hawq.mapreduce.util;

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


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class HAWQJdbcUtils {
	// prevent client to instantiate this class
	private HAWQJdbcUtils() { throw new AssertionError(); }

	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e.getMessage());
		}
	}

	public static Connection getConnection(String url, String username, String password)
			throws SQLException {
		return DriverManager.getConnection(url, username, password);
	}

	/**
	 * execute a query and return the result as a list of rows, each row is represented
	 * as column_name->column_value map. To against SQL-injection attack, the input sql
	 * must be safe, which normally means it's not constructed from user input.
	 *
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public static List<Map<String, String>> executeSafeQuery(Connection conn, String sql)
			throws SQLException {
		List<Map<String, String>> rows = Lists.newArrayList();

		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			ResultSetMetaData md = rs.getMetaData();
			final int numCols = md.getColumnCount();

			while (rs.next()) {
				Map<String, String> row = Maps.newHashMap();
				for (int i = 1; i <= numCols; ++i) {
					row.put(md.getColumnLabel(i), rs.getString(i));
				}
				rows.add(row);
			}

		} finally {
			free(stmt, rs);
		}

		return rows;
	}

	/**
	 * execute a safe query and return the first row of the result.
	 * If the query contains no result, return null.
	 *
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, String> executeSafeQueryForSingleRow(Connection conn, String sql)
			throws SQLException {
		List<Map<String, String>> rows = executeSafeQuery(conn, sql);
		return rows.size() == 0 ? null : rows.get(0);
	}

	public static void free(Statement stmt, ResultSet rs) throws SQLException {
		if (stmt != null) {
			stmt.close();
		}

		if (rs != null) {
			rs.close();
		}
	}

	public static void closeConnection(Connection conn) throws SQLException {
		if (conn != null) {
			conn.close();
		}
	}
}
