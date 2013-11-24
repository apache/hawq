package com.pxf.infra.hbase;

import java.security.Permission;
import java.util.ArrayList;

import jsystem.framework.system.SystemObjectImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.util.Bytes;

import com.pxf.infra.common.IDbFunctionality;
import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.structures.tables.basic.TableRecord;
import com.pxf.infra.structures.tables.hbase.HBaseTable;
import com.pxf.infra.structures.tables.hbase.HBaseTableRecord;
import com.pxf.infra.utils.jsystem.report.ReportUtils;

/**
 * HBase system object
 */
public class HBase extends SystemObjectImpl implements IDbFunctionality {

	private Configuration config;
	private HBaseAdmin admin;
	private String host;

	@Override
	public void init() throws Exception {

		super.init();

		connect();
	}

	/**
	 * connect to HBase
	 * 
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 */
	public void connect() throws MasterNotRunningException,
			ZooKeeperConnectionException {

		config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
	}

	@Override
	public ArrayList<String> getTableList(String schema) throws Exception {

		ReportUtils.startLevel(report, getClass(), "List Tables");

		HTableDescriptor[] tables = admin.listTables();

		ArrayList<String> tablesNames = new ArrayList<String>();

		for (int i = 0; i < tables.length; i++) {

			tablesNames.add(tables[i].getNameAsString());

		}

		ReportUtils.report(report, getClass(), tablesNames.toString());

		ReportUtils.stopLevel(report);

		return tablesNames;
	}

	/**
	 * Insert record to Table.
	 * 
	 * @param table
	 * @param record
	 * @throws Exception
	 */
	public void putRecord(Table table, TableRecord record) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Put data in Table: " + table.getName());

		HTable tbl = new HTable(config, table.getName());

		HBaseTableRecord hRecord = (HBaseTableRecord) record;

		Put p = new Put(Bytes.toBytes(hRecord.getRowKey()));

		p.add(Bytes.toBytes(hRecord.getColumnFamilyName()), Bytes.toBytes(hRecord.getCoulmnQualifier()), Bytes.toBytes(hRecord.getValue()));

		tbl.put(p);

		ReportUtils.stopLevel(report);
	}

	/**
	 * get value and store it in given Record
	 * 
	 * @param table
	 * @param record
	 * @throws Exception
	 */
	public void getRecord(Table table, TableRecord record) throws Exception {

		HBaseTableRecord hbaseTableRecord = (HBaseTableRecord) record;

		ReportUtils.startLevel(report, getClass(), "Get row " + hbaseTableRecord.getRowKey() + " from Table: " + table.getName());

		HTable tbl = new HTable(config, table.getName());

		Get g = new Get(Bytes.toBytes(hbaseTableRecord.getRowKey()));

		ReportUtils.report(report, getClass(), tbl.get(g).toString());

		ReportUtils.stopLevel(report);
	}

	@Override
	public void getTableData(Table table) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Scan Table: " + table.getName());

		HTable tbl = new HTable(config, table.getName());
		Scan scan = new Scan();

		ResultScanner rs = tbl.getScanner(scan);

		StringBuffer sb = new StringBuffer();

		for (Result result : rs) {

			sb.append(result.list());

		}

		ReportUtils.report(report, getClass(), sb.toString());

		ReportUtils.stopLevel(report);
	}

	/**
	 * Load Bulk of data using ImportTsv.
	 * 
	 * @param table
	 *            to load to
	 * @param inputPath
	 * @param cols
	 *            to which columns
	 * @throws Exception
	 */
	public void loadBulk(Table table, String inputPath, String... cols)
			throws Exception {

		ReportUtils.startLevel(report, getClass(), "Load Bulk from " + inputPath + " to Table: " + table.getName());

		ArrayList<String> argsList = new ArrayList<String>();

		StringBuilder sb = new StringBuilder();

		sb.append("-Dimporttsv.columns=HBASE_ROW_KEY,");

		for (int i = 0; i < cols.length; i++) {
			sb.append(cols[i]);
			if (i != cols.length - 1) {
				sb.append(",");
			}
		}

		argsList.add(sb.toString());
		argsList.add(table.getName());
		argsList.add("/" + inputPath);

		String[] args = new String[argsList.size()];

		for (int i = 0; i < argsList.size(); i++) {
			args[i] = argsList.get(i);
		}

		try {
			/**
			 * ImprtTav.main performing exit 0 when done. In order to prevent
			 * it, I have here a hack that catches the exit and preventing it.
			 */
			forbidSystemExitCall();
			ImportTsv.main(args);
		} catch (Exception e) {

			/**
			 * When this ExitTrappedException thrown , the exit 0 performed.
			 */
			if (e instanceof ExitTrappedException) {
				System.out.println("Prevent Exit VM 0");
			}
		}

		ReportUtils.stopLevel(report);
	}

	private static class ExitTrappedException extends SecurityException {

		private static final long serialVersionUID = 1L;
	}

	private static void forbidSystemExitCall() {
		final SecurityManager securityManager = new SecurityManager() {
			public void checkPermission(Permission permission) {

				if ("exitVM.0".equals(permission.getName())) {
					throw new ExitTrappedException();
				}
			}
		};
		System.setSecurityManager(securityManager);
	}

	private static void enableSystemExitCall() {
		System.setSecurityManager(null);
	}

	@Override
	public void createTable(Table table) throws Exception {

		HBaseTable hTable = (HBaseTable) table;

		ReportUtils.startLevel(report, getClass(), "Create Table " + table.getName());

		HTableDescriptor htd = new HTableDescriptor(table.getName());

		for (int i = 0; i < hTable.getFields().length; i++) {

			HColumnDescriptor hcd = new HColumnDescriptor(hTable.getFields()[i]);

			htd.addFamily(hcd);
		}

		admin.createTable(htd);

		ReportUtils.stopLevel(report);
	}

	@Override
	public void dropTable(Table table) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Remove Table: " + table.getName());

		admin.disableTable(table.getName());
		admin.deleteTable(table.getName());

		ReportUtils.stopLevel(report);
	}

	@Override
	public void dropDataBase(String schemaName, boolean cascade, boolean ignoreFail)
			throws Exception {

		ReportUtils.throwUnsupportedFunctionality(getClass(), "Drop Schema");

	}

	@Override
	public void insertData(Table source, Table target) throws Exception {

		ReportUtils.throwUnsupportedFunctionality(getClass(), "Insert Data");

	}

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail)
			throws Exception {

		ReportUtils.throwUnsupportedFunctionality(getClass(), "Create Schema");

	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
}
