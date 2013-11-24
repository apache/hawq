package com.pxf.infra.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import jsystem.framework.report.Reporter;
import jsystem.framework.system.SystemObjectImpl;

import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.utils.exception.ExceptionUtils;
import com.pxf.infra.utils.jsystem.report.ReportUtils;

/**
 * System object for interacting DB via JDBC. Every System Object that
 * represents JDBC DB should extend from it.
 * 
 */
public class DbSystemObject extends SystemObjectImpl implements
		IDbFunctionality {

	protected String driver;
	protected String address;

	protected Connection dbConnection;
	protected Statement stmt;

	/**
	 * Connect to DB
	 * 
	 * @throws Exception
	 */
	public void connect() throws Exception {

		if (stmt == null) {
			Class.forName(driver);

			dbConnection = DriverManager.getConnection(address);

			stmt = dbConnection.createStatement();
		}
	}

	@Override
	public void createTable(Table table) throws Exception {
		runQuery(table.constructCreateStmt());

	}

	@Override
	public void dropTable(Table table) throws Exception {

		runQueryWithExpectedWarning(table.constructDropStmt(), "table \"" + table.getName() + "\" does not exist, skipping", true, true);
	}

	@Override
	public void dropDataBase(String schemaName, boolean cascade, boolean ignoreFail)
			throws Exception {

		runQuery("DROP SCHEMA " + schemaName + ((cascade) ? " CASCADE" : ""), ignoreFail);

	}

	@Override
	public void insertData(Table source, Table target) throws Exception {

		StringBuilder dataStringBuilder = new StringBuilder();

		List<List<String>> data = source.getData();

		int dataSize = data.size();

		for (int i = 0; i < dataSize; i++) {

			List<String> row = data.get(i);

			dataStringBuilder.append("(");

			int rowSize = row.size();

			for (int j = 0; j < rowSize; j++) {

				dataStringBuilder.append("'" + row.get(j) + "'");

				if (j != rowSize - 1) {
					dataStringBuilder.append(",");
				}
			}

			dataStringBuilder.append(")");

			if (i != dataSize - 1) {
				dataStringBuilder.append(",");
			}
		}

		runQuery("INSERT INTO " + target.getName() + " VALUES " + dataStringBuilder);
	}

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail)
			throws Exception {

		runQuery("CREATE SCHEMA " + schemaName, ignoreFail);

	}

	public void runQuery(String query) throws Exception {
		runQuery(query, false);
	}

	public void runQueryWithExpectedWarning(String query, String expectedWarning, boolean isRegex, boolean ignoreNoWarning)
			throws Exception {

		runQuery(query, true);

		verifyWarning(expectedWarning, isRegex, ignoreNoWarning);
	}

	public void runQueryWithExpectedWarning(String query, String expectedWarning, boolean isRegex)
			throws Exception {

		runQueryWithExpectedWarning(query, expectedWarning, isRegex, false);
	}

	/**
	 * Run single execute query.
	 * 
	 * @param query
	 *            string query
	 * @param ignoreFail
	 *            if true will ignore query execution failure.
	 * @throws Exception
	 */
	public void runQuery(String query, boolean ignoreFail) throws Exception {

		ReportUtils.report(report, getClass(), query);

		try {
			long startTimeInMillis = System.currentTimeMillis();
			stmt.execute(query);
			ReportUtils.report(report, getClass(), "Run in " + (System.currentTimeMillis() - startTimeInMillis) + " miliseconds");

			if (stmt.getWarnings() != null) {
				throw stmt.getWarnings();
			}
		} catch (SQLException e) {
			if (!ignoreFail) {
				throw e;
			}
		}
	}

	/**
	 * Query results from DB into Table structure.
	 * 
	 * @param table
	 * @param query
	 * @throws Exception
	 */
	public void queryResults(Table table, String query) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Query results: " + query);

		long startTimeInMillis = System.currentTimeMillis();
		ResultSet res = stmt.executeQuery(query);
		ReportUtils.report(report, getClass(), "Run in " + (System.currentTimeMillis() - startTimeInMillis) + " miliseconds");

		boolean readMetaData = true;

		table.init();

		while (res.next()) {

			ArrayList<String> currentRow = new ArrayList<String>();

			for (int j = 1; j <= res.getMetaData().getColumnCount(); j++) {

				currentRow.add(res.getString(j));

				if (readMetaData) {
					table.addColDataType(res.getMetaData().getColumnType(j));
					table.addColumnHeader(res.getMetaData().getColumnName(j));
				}
			}

			readMetaData = false;

			table.addRow(currentRow);
		}

		ReportUtils.report(report, getClass(), table.getColsDataType()
				.toString());
		ReportUtils.reportHtml(report, getClass(), table.getDataHtml());

		ReportUtils.stopLevel(report);
	}

	/**
	 * Check if Table exists in specific Schema.
	 * 
	 * @param schemaName
	 * @param tblName
	 * @return true if exists.
	 * @throws IOException
	 * @throws SQLException
	 */
	public boolean checkTableExists(String schemaName, String tblName)
			throws IOException, SQLException {

		ResultSet res = getTablesForSchema(schemaName, tblName);

		if (res.next()) {
			return true;
		}

		return false;
	}

	@Override
	public ArrayList<String> getTableList(String schema) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Get All Tables for schema: " + schema);

		ArrayList<String> list = new ArrayList<String>();

		ResultSet res = getTablesForSchema(schema, null);

		while (res.next()) {
			list.add(res.getString(3));
		}

		report.report(list.toString());

		ReportUtils.stopLevel(report);

		return list;
	}

	/**
	 * 
	 * @param schema
	 * @param table
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getTablesForSchema(String schema, String table)
			throws SQLException {

		DatabaseMetaData metaData = dbConnection.getMetaData();

		String tableName = "";

		if (table != null) {
			tableName = table;
		}

		return metaData.getTables(schema, schema, "%" + tableName, new String[] { "TABLE" });

	}

	@Override
	public void getTableData(Table table) throws Exception {
		ReportUtils.throwUnsupportedFunctionality(getClass(), "Get All Table");
	}

	@Override
	public void close() {

		if (dbConnection != null) {
			try {
				dbConnection.close();
				stmt = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		super.close();
	}

	public void verifyWarning(String expectedWarningMessage, boolean isRegex, boolean ignorNoWarning)
			throws SQLException, IOException {

		if (stmt.getWarnings() == null) {

			if (!ignorNoWarning) {
				ReportUtils.report(report, getClass(), "Expected Warning: " + expectedWarningMessage + ": No Warnings thrown", Reporter.WARNING);
			}
			return;
		}

		ExceptionUtils.validate(report, stmt.getWarnings(), new Exception(expectedWarningMessage), isRegex, true);
	}
}
