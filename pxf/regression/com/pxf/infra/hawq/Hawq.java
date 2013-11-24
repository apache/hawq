package com.pxf.infra.hawq;

import java.util.ArrayList;
import java.util.List;

import com.aqua.sysobj.conn.CliCommand;
import com.pxf.infra.common.DbSystemObject;
import com.pxf.infra.common.ShellSystemObject;
import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.infra.utils.jsystem.sut.SutUtils;

/**
 * HAWQ system object.
 * 
 */
public class Hawq extends DbSystemObject {

	private String host;

	private String db;

	private String sshUserName;

	private String sshPassword;

	@Override
	public void init() throws Exception {

		/**
		 * Connect using default "template1" database for creating the required
		 * database and connecting it.
		 */
		setDb(SutUtils.getValue(sut, "//hawq//db"));
		setHost(SutUtils.getValue(sut, "//hawq//host"));

		driver = "org.postgresql.Driver";
		address = "jdbc:postgresql://" + getHost() + ":5432/template1";

		super.init();

		connect();

		createDataBase(getDb(), true);

		super.close();
		address = "jdbc:postgresql://" + getHost() + ":5432/" + getDb();

		connect();
	}

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail)
			throws Exception {

		runQuery("CREATE DATABASE " + schemaName, ignoreFail);
	}

	@Override
	public void dropDataBase(String schemaName, boolean cascade, boolean ignoreFail)
			throws Exception {

		runQuery("DROP DATABASE " + schemaName, ignoreFail);
	}

	/**
	 * Copy data from "from" table to "to" table using cli STDIN
	 * 
	 * @param from
	 *            copy from required Table data
	 * @param to
	 *            copy to required table
	 * @throws Exception
	 */
	public void copy(Table from, Table to) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Copy from " + from.getFullName() + " to " + to.getFullName());

		ShellSystemObject sso = new ShellSystemObject();

		sso.setHost(getHost());
		sso.setUserName(getSshUserName());
		sso.setPassword(getSshPassword());

		sso.init();
		CliCommand command = new CliCommand();
		command.setTimeout(100);
		command.setCommand("source $GPHOME/greenplum_path.sh");
		sso.command(command);
		command.setCommand("psql " + getDb());
		sso.command(command);
		command.setCommand("COPY " + to.getName() + " FROM STDIN DELIMITER ',';");
		sso.command(command);

		StringBuilder dataStringBuilder = new StringBuilder();

		List<List<String>> data = from.getData();

		for (int i = 0; i < data.size(); i++) {

			List<String> row = data.get(i);

			for (int j = 0; j < row.size(); j++) {

				dataStringBuilder.append(row.get(j));

				if (j != row.size() - 1) {
					dataStringBuilder.append(",");
				}
			}

			dataStringBuilder.append("\n");

		}

		dataStringBuilder.append("\\.");

		command.setCommand(dataStringBuilder.toString());
		sso.command(command);
		sso.disconnect();

		ReportUtils.stopLevel(report);
	}

	/**
	 * Get list of all schemas exists.
	 * 
	 * @return
	 * @throws Exception
	 */
	public ArrayList<String> getDataBasesList() throws Exception {

		Table dataname = new Table("dataname", null);
		queryResults(dataname, "SELECT datname FROM pg_catalog.pg_database");

		ArrayList<String> dbList = new ArrayList<String>();

		for (List<String> row : dataname.getData()) {

			dbList.add(row.get(0));

		}

		return dbList;
	}

	public void analyze(Table table) throws Exception {
		analyze(table, false);
	}

	public void analyze(Table table, boolean expectOffWarning) throws Exception {

		String query = "ANALYZE " + table.getName();

		if (expectOffWarning) {
			runQueryWithExpectedWarning(query, "analyze for PXF tables is turned off by 'pxf_enable_stat_collection'", true);

		} else {
			runQuery(query);
		}
	}

	public String getSshUserName() {
		return sshUserName;
	}

	public void setSshUserName(String sshUserName) {
		this.sshUserName = sshUserName;
	}

	public String getSshPassword() {
		return sshPassword;
	}

	public void setSshPassword(String sshPassword) {
		this.sshPassword = sshPassword;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}
}
