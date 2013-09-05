package com.pivotal.hawq.mapreduce.ao.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public abstract class Register {
	protected Exception lastException = null;
	protected boolean normalExit = false;
	protected boolean aborted = false;
	protected String source = "";
	protected String statisticFile = "";
	protected String className = "";
	protected String username = System.getProperty("user.name");
	protected Database database = null;
	private FileSystem filesystem = null;
	private String hostname = "";
	private HashMap<String, Metadata> statistics = new HashMap<String, Metadata>();

	protected abstract void printUsage();

	protected abstract void register(String[] args) throws Exception;

	private void closeDatabase() {
		if (database != null) {
			database.close();
			database = null;
		}
	}

	private void closeFileSystem() {
		if (filesystem != null) {
			try {
				filesystem.close();
			} catch (IOException e) {
			}
			filesystem = null;
		}
	}

	protected void printlog(String type, String log) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSSSSS");
		String prefix = "";
		if (hostname.equals("")) {
			try {
				hostname = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				hostname = "UnknownHost";
			}
		}
		prefix = sdf.format(new Date()) + " " + className + ":" + hostname
				+ ":" + username;
		if (type.equals("info"))
			System.out.println(prefix + "-[INFO]:-" + log);
		else if (type.equals("error"))
			System.err.println(prefix + "-[ERROR]:-" + log);
		else if (type.equals("warn"))
			System.err.println(prefix + "-[WARN]:-" + log);
		else
			System.err.println(prefix + "-[WRONG]:-" + log);
	}

	protected void connectToHdfs() throws SQLException, IOException {
		// Get HDFS uri from database table pg_filespace_entry
		String hdfsUri = database.getHdfsPath();
		if (hdfsUri.equals(""))
			throw new SQLException("Failed to get path of hdfs.");

		// Try to connect to HDFS
		printlog("info", "Try to connect " + hdfsUri);
		Configuration conf = new Configuration();
		filesystem = FileSystem.get(URI.create(hdfsUri), conf);
		if (filesystem == null)
			throw new IOException("Cannot connect to " + hdfsUri);
		printlog("info", "Succeed to connect HDFS: " + hdfsUri);
	}

	protected void checkFileAndStatistic() throws IOException, SQLException {
		// Make sure statistics file exists, get amount of source files
		int fileAmountWithStatistic = filesystem.listStatus(new Path(source)).length;
		if (!filesystem.exists(new Path(statisticFile)))
			throw new IOException("Statistic file " + statisticFile
					+ " does not exist");
		if (fileAmountWithStatistic == 1)
			throw new IOException("No input file in source");
		printlog("info", "Total " + fileAmountWithStatistic + " file(s) in "
				+ source);

		// Get amount of segments from database gp_segment_configuration
		int segAmount = database.getSegAmount();
		if (segAmount == -1)
			throw new IOException("Failed to get amount of segments");
		printlog("info", "Total " + segAmount + " segment(s)");

		// Check amount of segments and source files
		if (fileAmountWithStatistic > 128 * segAmount)
			throw new IOException("There are " + fileAmountWithStatistic
					+ " files in source, while database can hold " + 127
					* segAmount + " files");
	}

	protected void analyzeAndRemoveStatisticFile() throws IOException {
		printlog("info", "Analyze statistic file");
		int fileAmountWithStatistic = filesystem.listStatus(new Path(source)).length;
		FSDataInputStream inputStream = filesystem
				.open(new Path(statisticFile));
		long length = filesystem.getFileStatus(new Path(statisticFile))
				.getLen();
		byte[] buffer = new byte[(int) length];
		inputStream.readFully(0, buffer);
		inputStream.close();
		String content = new String(buffer);
		String[] items = content.split("\n");
		if (items.length < fileAmountWithStatistic - 1)
			throw new IOException("There are " + (fileAmountWithStatistic - 1)
					+ " input file(s), while only " + items.length
					+ " statistics data(s)");
		for (int i = 0; i < items.length; i++) {
			String[] pairs = items[i].split("\t");
			if (pairs.length < 6)
				throw new IOException("Wrong format of statistics data '"
						+ items[i] + "'");
			statistics.put(pairs[0], new Metadata(pairs[1], pairs[2], pairs[3],
					pairs[4], pairs[5]));
		}
		filesystem.delete(new Path(statisticFile), false);
	}

	protected void registerPartition(String newPartitionTableName)
			throws IOException, SQLException {
		registerTable(newPartitionTableName);
	}

	protected void registerTable(String newTableName) throws IOException,
			SQLException {
		// Get file path of segments in hdfs
		String[] segPaths = database.getSegPaths();
		if (segPaths.length == 0)
			throw new SQLException("Failed to get path of table "
					+ newTableName);
		printlog("info", "Get File path of table " + newTableName);

		// Get file path of table in each segment
		int tablespace = database.getTablespaceId();
		int databaseOid = database.getDatabaseOid();
		int tableOid = database.getTableOid(newTableName);
		int tableFilenode = database.getTableFileNode(tableOid);
		if (tablespace == -1 || databaseOid == -1 || tableFilenode == -1)
			throw new SQLException("Failed to get path of table "
					+ newTableName);
		String tablePath = "/" + tablespace + "/" + databaseOid + "/"
				+ tableFilenode + ".";

		// Get oid of pg_aoseg, pg_aoseg_index and new table
		printlog("info",
				"Move source files into database and modify system table");
		int pgaosegOid = database.getPgaosegOid(tableFilenode);
		int pgaosegIndexOid = database.getPgaosegIndexOid(tableFilenode);
		if (tableFilenode == -1 || pgaosegOid == -1 || pgaosegIndexOid == -1)
			throw new SQLException(
					"Failed to get oid of pg_aoseg or pg_aoseg_index or table "
							+ newTableName);

		// Move source files into database and modify system table
		FileStatus[] sourceFileList = filesystem.listStatus(new Path(source));
		int fileAmount = sourceFileList.length;
		printlog("info", "  Please wait...");
		int segAmount = database.getSegAmount();
		int filesPerSeg = (fileAmount - 1) / segAmount + 1;
		for (int j = 1; j <= filesPerSeg; j++) {
			int tupcountForMaster = 0;
			for (int i = 0; i < segAmount; i++) {
				if (fileAmount == 0) {
					database.modifySystemTable(pgaosegOid, pgaosegIndexOid, j,
							i, 0, 0, 0, 0, 0, tableFilenode);
				} else {
					String oldPath = sourceFileList[fileAmount - 1].getPath()
							.toString();
					if (!sourceFileList[fileAmount - 1].isFile())
						throw new SQLException(oldPath + " is not a file!");
					Metadata data = statistics.get(oldPath.substring(oldPath
							.lastIndexOf('/') + 1));
					if (data == null)
						throw new IOException(
								"Cannot get statistic data for file " + oldPath);
					int tupcount = data.tupcount, varblockcount = data.varblockcount, last_sequence = data.last_sequence;
					long eof = data.eof, eofuncompressed = data.eofuncompressed;
					tupcountForMaster += tupcount;
					database.modifySystemTable(pgaosegOid, pgaosegIndexOid, j,
							i, eof, tupcount, varblockcount, eofuncompressed,
							last_sequence, tableFilenode);
					String newPath = segPaths[i] + tablePath + j;
					filesystem.delete(new Path(newPath), false);
					filesystem.rename(new Path(oldPath), new Path(newPath));
					filesystem.setPermission(new Path(newPath),
							new FsPermission(FsAction.READ_WRITE,
									FsAction.NONE, FsAction.NONE));
					fileAmount--;
				}
			}
			database.modifySystemTable(pgaosegOid, pgaosegIndexOid, j, -1, 0,
					tupcountForMaster, 0, 0, 0, tableFilenode);
		}
	}

	protected void vacuumAnalyze(String tableName) {
		// Vacuum analyze
		printlog("info", "Vacuum analyze " + tableName + ", please wait...");
		try {
			database.vacuumAnalyze(tableName);
			printlog("info", "Vacuum suucced");
		} catch (SQLException e) {
			printlog("warn", "Vacuum failed: " + e.getMessage());
			e.printStackTrace();
		}
	}

	protected final class ExitHandler extends Thread {
		public boolean needRollback = false;

		public ExitHandler() {
			super("Exit Handler");
		}

		public void run() {
			if (!normalExit) {
				aborted = true;
				if (lastException != null)
					printlog("error", "Get some error. Exiting and cleaning...");
				else
					printlog("error", "Aborted...");
				if (database != null && database.connectionIsValid()
						&& needRollback) {
					printlog("warn", "Rollback...");
					try {
						if (needRollback)
							database.transaction('r');
					} catch (SQLException e) {
						printlog("error", e.getMessage());
						printlog("error", "Fail to rollback");
					}
				}
				printlog("error", "RegisterNewAOTable exited");
				if (lastException != null) {
					printlog("error", lastException.getMessage());
					lastException = null;
				}
			}
			closeDatabase();
			closeFileSystem();
		}
	}

	private final class Metadata {
		public long eof;
		public int tupcount;
		public int varblockcount;
		public long eofuncompressed;
		public int last_sequence;

		public Metadata(String eof, String tupcount, String varblockcount,
						String eofuncompressed, String last_sequence) {
			this.eof = Long.parseLong(eof);
			this.tupcount = Integer.parseInt(tupcount);
			this.varblockcount = Integer.parseInt(varblockcount);
			this.eofuncompressed = Long.parseLong(eofuncompressed);
			this.last_sequence = Integer.parseInt(last_sequence);
		}
	}
}