package com.pxf.infra.hive;

import com.pxf.infra.common.DbSystemObject;
import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.structures.tables.hive.HiveTable;
import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.infra.utils.jsystem.sut.SutUtils;

/**
 * Hive System Object
 * 
 */
public class Hive extends DbSystemObject {

	String host;
	String port = "10004";

	@Override
	public void init() throws Exception {

		setHost(SutUtils.getValue(sut, "//hive//host"));

		String sutPort = SutUtils.getValue(sut, "//hive/port");

		if (sutPort != null && !sutPort.equals("null") && !sutPort.equals("")) {
			setPort(sutPort);
		}

		driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
		address = "jdbc:hive://" + host + "/" + port;

		super.init();

		connect();
	}

	/**
	 * load data to Table from file.
	 * 
	 * @param table
	 * @param filePath
	 * @throws Exception
	 */
	public void loadData(HiveTable table, String filePath) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Load Data from " + filePath + " to Table: " + table.getName());

		String sql = "LOAD DATA LOCAL INPATH '" + filePath + "' INTO TABLE " + table.getName();

		stmt.executeQuery(sql);

		ReportUtils.stopLevel(report);
	}

	@Override
	public void dropTable(Table table) throws Exception {
		runQuery(table.constructDropStmt());
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}
}