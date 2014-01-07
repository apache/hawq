package com.pivotal.hawq.mapreduce.ao.db;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;

import java.sql.*;
import java.util.ArrayList;

/**
 * This class is encapsulation of HAWQ database and methods in this class supply
 * way to get metadata and other information about table and database.
 */
public final class Database
{
	private String dbName;
	private Connection conn = null;
	private Statement statement = null;
	private ResultSet resultSet = null;
	private int segAmount = 0;

	public static enum TableType
	{
		AO_TABLE, CO_TABLE, PARQUET_TABLE
	}

	public Database()
	{}

	/**
	 * Connect to database
	 * 
	 * @param db_url
	 *            url of database, e.g. localhost:5432/postgres
	 * @param username
	 *            user name
	 * @param password
	 *            password
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void connectToDatabase(String db_url, String username,
			String password) throws SQLException, ClassNotFoundException
	{
		try
		{
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection("jdbc:postgresql://" + db_url,
					username, password);
			statement = conn.createStatement();
			resultSet = statement.executeQuery("SELECT current_database()");
			resultSet.next();
			this.dbName = resultSet.getString(1);
		}
		catch (ClassNotFoundException e)
		{
			close();
			throw e;
		}
		catch (SQLException e)
		{
			close();
			throw e;
		}
	}

	/**
	 * Check whether this connection is valid
	 * 
	 * @return whether this connection is valid
	 */
	public boolean connectionIsValid()
	{
		return (conn != null);
	}

	public void close()
	{
		try
		{
			if (statement != null)
			{
				statement.close();
				statement = null;
			}
			if (resultSet != null)
			{
				resultSet.close();
				resultSet = null;
			}
			if (conn != null)
			{
				conn.close();
				conn = null;
			}
		}
		catch (SQLException e)
		{}
	}

	/**
	 * Create table in database
	 * 
	 * @param tableFormat
	 *            table string, e.g. a(a int,b float)
	 * @throws SQLException
	 */
	public void createTable(String tableFormat) throws SQLException
	{
		statement.execute("CREATE TABLE " + tableFormat);
	}

	/**
	 * Add new partition to existing table
	 * 
	 * @param existTable
	 *            name of existing table
	 * @param newPartitionName
	 *            name of new partition
	 * @param lowerBound
	 *            start of partition
	 * @param upperBound
	 *            end of partition
	 * @throws SQLException
	 */
	public void addPartition(String existTable, String newPartitionName,
			String lowerBound, String upperBound) throws SQLException
	{
		statement.execute("ALTER TABLE " + existTable + " ADD PARTITION "
				+ newPartitionName + " START " + lowerBound + " END "
				+ upperBound);
	}

	/**
	 * Begin/submit/rollback a transaction
	 * 
	 * @param action
	 *            'b' means begin a transaction, 'c' means commit a transaction,
	 *            'r' means rollback a transaction
	 * @throws SQLException
	 */
	public void transaction(char action) throws SQLException
	{
		switch (action)
		{
		case 'b':
			statement.execute("BEGIN");
			break;
		case 'c':
			statement.execute("COMMIT");
			break;
		case 'r':
			statement.execute("ROLLBACK");
			break;
		}
	}

	private int gp_register_table_partition_lock(int arg0, int arg1, int arg2,
			int arg3, long arg4, int arg5, int arg6, long arg7, int arg8,
			int arg9) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT gp_register_table_partition_lock(" + arg0
						+ "," + arg1 + "," + arg2 + "," + arg3 + "," + arg4
						+ "," + arg5 + "," + arg6 + "," + arg7 + "," + arg8
						+ "," + arg9 + ")");
		int result = 1;
		if (resultSet.next())
			result = resultSet.getInt(1);
		return result;
	}

	/**
	 * Modify metadata table for append only table, user should not touch this
	 * function
	 * 
	 * @param pgaosegOid
	 * @param pgaosegIndexOid
	 * @param fileno
	 * @param segno
	 * @param eof
	 * @param tupcount
	 * @param varblockcount
	 * @param eofuncompressed
	 * @param last_sequence
	 * @param tableOid
	 * @throws SQLException
	 */
	public void modifySystemTable(int pgaosegOid, int pgaosegIndexOid,
			int fileno, int segno, long eof, int tupcount, int varblockcount,
			long eofuncompressed, int last_sequence, int tableOid)
			throws SQLException
	{
		int result = gp_register_table_partition_lock(pgaosegOid,
				pgaosegIndexOid, fileno, segno, eof, tupcount, varblockcount,
				eofuncompressed, last_sequence, tableOid);
		if (result != 0)
			throw new SQLException("Failed to modify system table");
	}

	/**
	 * Lock the table, user should not touch this function
	 * 
	 * @param mode
	 * @param tableOid
	 * @throws SQLException
	 */
	public void tableLock(int mode, int tableOid) throws SQLException
	{
		int lock = -1;
		if (mode == 0) // Lock
			lock = 345;
		else if (mode == 1) // Unlock
			lock = 456;
		else
		{
			throw new SQLException("Wrong mode for lock/unlock");
		}
		int result = this.gp_register_table_partition_lock(-1, lock, 0, 0, 0,
				0, 0, 0, 0, tableOid);
		if (result != 0)
		{
			throw new SQLException("Failed to lock/unlock table");
		}
	}

	/**
	 * Call vacuum analyze in database
	 * 
	 * @param tableName
	 *            name of table to vacuum analyze
	 * @throws SQLException
	 */
	public void vacuumAnalyze(String tableName) throws SQLException
	{
		statement.execute("VACUUM ANALYZE " + tableName);
	}

	/**
	 * Get hdfs path of database
	 * 
	 * @return hdfs path of database
	 * @throws SQLException
	 */
	public String getHdfsPath() throws SQLException
	{
		String filepathSql = "SELECT fselocation from pg_filespace_entry where fsedbid>1"
				+ " AND fsefsoid=(SELECT oid from pg_filespace where fsname='dfs_system')";
		resultSet = statement.executeQuery(filepathSql);
		while (resultSet.next())
		{
			String filepath = resultSet.getString(1);
			if (filepath.startsWith("hdfs"))
			{
				return filepath.substring(0,
						filepath.indexOf('/', filepath.indexOf("//") + 3));
			}
		}
		return "";
	}

	/**
	 * Get amount of segment in this database
	 * 
	 * @return amount of segment
	 * @throws SQLException
	 */
	public int getSegAmount() throws SQLException
	{
		if (segAmount == 0)
		{
			resultSet = statement
					.executeQuery("SELECT COUNT(*) FROM gp_segment_configuration WHERE content>=0");
			if (resultSet.next())
				segAmount = resultSet.getInt(1);
			else
				segAmount = -1;
		}
		return segAmount;
	}

	/**
	 * Get list of segment hdfs path in this database
	 * 
	 * @return list of segment hdfs path
	 * @throws SQLException
	 */
	public String[] getSegPaths() throws SQLException
	{
		if (segAmount == 0)
			getSegAmount();
		String[] segPath = new String[segAmount];
		for (int i = 0; i < segAmount; i++)
		{
			resultSet = statement
					.executeQuery("SELECT fselocation FROM pg_filespace_entry"
							+ " WHERE fsefsoid=(SELECT oid FROM pg_filespace WHERE fsname='dfs_system')"
							+ " AND fsedbid=(SELECT dbid FROM gp_segment_configuration WHERE content="
							+ i + ")");
			if (resultSet.next())
				segPath[i] = resultSet.getString(1);
		}
		for (int i = 0; i < segAmount; i++)
			segPath[i] = segPath[i].substring(segPath[i].indexOf('/',
					segPath[i].indexOf("//") + 3));
		return segPath;
	}

	/**
	 * Get id of table space where this database is in
	 * 
	 * @return id of table space for this database
	 * @throws SQLException
	 */
	public int getTablespaceId() throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT dat2tablespace FROM pg_database WHERE datname = '"
						+ dbName + "'");
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	/**
	 * Get oid of database
	 * 
	 * @return oid of database
	 * @throws SQLException
	 */
	public int getDatabaseOid() throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT oid FROM pg_database WHERE datname = '"
						+ dbName + "'");
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	/**
	 * Get encoding of database
	 * 
	 * @return encoding of database
	 * @throws SQLException
	 */
	public String getDatabaseEncoding() throws SQLException
	{
		resultSet = statement.executeQuery("SELECT getdatabaseencoding()");
		if (resultSet.next())
			return resultSet.getString(1);
		else
			return "";
	}

	/**
	 * Get oid of pg_aoseg.pg_aoseg_* table
	 * 
	 * @param tableFilenode
	 *            filenode of table
	 * @return oid
	 * @throws SQLException
	 */
	public int getPgaosegOid(int tableFilenode) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT oid FROM pg_class WHERE relname = 'pg_aoseg_"
						+ tableFilenode + "'");
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	/**
	 * Get oid of pg_aoseg.pg_aoseg_*_index table
	 * 
	 * @param tableFilenode
	 *            filenode of table
	 * @return oid
	 * @throws SQLException
	 */
	public int getPgaosegIndexOid(int tableFilenode) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT oid FROM pg_class WHERE relname = 'pg_aoseg_"
						+ tableFilenode + "_index'");
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	/**
	 * Get oid of table by name
	 * 
	 * @param tableName
	 *            name of table
	 * @return oid of table
	 * @throws SQLException
	 */
	public int getTableOid(String tableName) throws SQLException
	{
		resultSet = statement.executeQuery("SELECT '" + tableName
				+ "'::regclass::oid");
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			throw new SQLException("Failed to get oid of table " + tableName);
	}

	/**
	 * GPSQL-972
	 * 
	 * Get type of table (AO_TABLE, CO_TABLE, PARQUET_TABLE)
	 * 
	 * @param tableOid
	 * @return type of table
	 * @throws SQLException
	 */
	public TableType getTableType(int tableOid) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT reloptions from pg_class where oid="
						+ tableOid);
		resultSet.next();
		String options = resultSet.getString(1);
		if (options.indexOf("appendonly=true") != -1)
		{
			if (options.indexOf("orientation=column") != -1)
				return TableType.CO_TABLE;
			else
				return TableType.AO_TABLE;
		}
		else
			return TableType.PARQUET_TABLE;
	}

	/**
	 * Get schmea of table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @param tableName
	 *            name of table
	 * @return schema of table
	 * @throws SQLException
	 * @throws HAWQException
	 */
	public HAWQSchema getTableSchema(int tableOid, String tableName)
			throws SQLException, HAWQException
	{
		resultSet = statement
				.executeQuery("SELECT oid FROM pg_namespace WHERE nspname='pg_catalog'");
		resultSet.next();
		int catalogOid = resultSet.getInt(1);
		ArrayList<HAWQField> fields = new ArrayList<HAWQField>();
		for (int i = 1;; i++)
		{
			resultSet = statement
					.executeQuery("SELECT attname from pg_attribute where attrelid="
							+ tableOid + " and attnum=" + i);
			if (!resultSet.next())
				break;
			String attname = resultSet.getString(1);

			resultSet = statement
					.executeQuery("SELECT typname,typnamespace from pg_type where oid="
							+ "(SELECT atttypid from pg_attribute where attrelid="
							+ tableOid + " and attnum=" + i + ")");
			resultSet.next();
			String fieldStr = resultSet.getString(1);
			int typnamespace = resultSet.getInt(2);
			if (typnamespace != catalogOid)
				throw new HAWQException("Type " + fieldStr
						+ " is not in pg_catalog and is not supported yet");
			try
			{
				if (fieldStr.startsWith("_"))
				{
					// array type
					if (fieldStr.equals("_int4") || fieldStr.equals("_int8")
							|| fieldStr.equals("_int2")
							|| fieldStr.equals("_float4")
							|| fieldStr.equals("_float8")
							|| fieldStr.equals("_bool")
							|| fieldStr.equals("_time")
							|| fieldStr.equals("_date")
							|| fieldStr.equals("_interval"))
					{
						HAWQPrimitiveField.PrimitiveType type = HAWQPrimitiveField.PrimitiveType
								.valueOf(fieldStr.substring(1).toUpperCase());
						fields.add(HAWQSchema.optional_field_array(type,
								attname));
					}
					else
						throw new HAWQException(fieldStr
								+ " is not supported yet.");
				}
				else
				{
					HAWQPrimitiveField.PrimitiveType type = HAWQPrimitiveField.PrimitiveType
							.valueOf(fieldStr.toUpperCase());
					fields.add(HAWQSchema.optional_field(type, attname));
				}
			}
			catch (IllegalArgumentException e)
			{
				throw new HAWQException(fieldStr + " is not supported yet.");
			}
		}
		return new HAWQSchema(tableName, fields);
	}

	/**
	 * Get block size of table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @return block size of table
	 * @throws SQLException
	 */
	public int getTableBlocksize(int tableOid) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT blocksize FROM pg_appendonly WHERE relid="
						+ tableOid);
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	/**
	 * Get compress level of table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @return compress level of table
	 * @throws SQLException
	 */
	public int getTableCompresslevel(int tableOid) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT compresslevel FROM pg_appendonly WHERE relid="
						+ tableOid);
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	/**
	 * Get checksum of table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @return checksum of table
	 * @throws SQLException
	 */
	public boolean getTableChecksum(int tableOid) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT checksum FROM pg_appendonly WHERE relid="
						+ tableOid);
		String checksumStr;
		if (resultSet.next())
			checksumStr = resultSet.getString(1).toLowerCase();
		else
			checksumStr = "";

		if (checksumStr.equals("t"))
			return true;
		else
			return false;
	}

	/**
	 * Get compress type of table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @return compress type of table
	 * @throws SQLException
	 */
	public String getTableCompresstype(int tableOid) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT compresstype FROM pg_appendonly WHERE relid="
						+ tableOid);
		String compressType;
		if (resultSet.next())
			compressType = resultSet.getString(1);
		else
			compressType = "";
		if (compressType == null || compressType.equals(""))
			compressType = "none";
		return compressType.toLowerCase();
	}

	/**
	 * Get file node of table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @return file node of table
	 * @throws SQLException
	 */
	public int getTableFileNode(int tableOid) throws SQLException
	{
		resultSet = statement
				.executeQuery("SELECT relfilenode FROM pg_class WHERE oid="
						+ tableOid);
		if (resultSet.next())
			return resultSet.getInt(1);
		else
			return -1;
	}

	private ArrayList<HAWQAOFileStatus> getAOTableFileAttrList(int tableOid)
			throws SQLException
	{
		int tableFileNode = getTableFileNode(tableOid);
		boolean checksum = getTableChecksum(tableOid);
		String compressType = getTableCompresstype(tableOid);
		int blocksize = getTableBlocksize(tableOid);
		resultSet = statement
				.executeQuery("SELECT COUNT(*) FROM pg_aoseg.pg_aoseg_"
						+ tableOid + " WHERE content>=0");
		resultSet.next();
		int fileNum = resultSet.getInt(1);
		ArrayList<HAWQAOFileStatus> aosplits = new ArrayList<HAWQAOFileStatus>();
		if (fileNum != 0)
		{
			String[] segPath = this.getSegPaths();
			int tablespaceId = this.getTablespaceId();
			int dboid = this.getDatabaseOid();
			resultSet = statement
					.executeQuery("SELECT segno,content,eof FROM pg_aoseg.pg_aoseg_"
							+ tableOid + " WHERE content>=0");
			String tablePath = "/" + tablespaceId + "/" + dboid + "/"
					+ tableFileNode;
			while (resultSet.next())
			{
				int segno = resultSet.getInt(2);
				int fileid = resultSet.getInt(1);
				long fileLength = resultSet.getLong(3);
				String path;
				if (fileid != 0)
					path = segPath[segno] + tablePath + "." + fileid;
				else
					path = segPath[segno] + tablePath;
				aosplits.add(new HAWQAOFileStatus(path, fileLength, checksum,
						compressType, blocksize));
			}
		}

		// Get file attributes of sub/partition table(s)
		resultSet = statement
				.executeQuery("SELECT inhrelid from pg_inherits where inhparent="
						+ tableOid);
		ArrayList<Integer> subtableOids = new ArrayList<Integer>();
		while (resultSet.next())
			subtableOids.add(resultSet.getInt(1));
		int subtableNum = subtableOids.size();
		for (int i = 0; i < subtableNum; i++)
			aosplits.addAll(getAOTableFileAttrList(subtableOids.get(i)));

		return aosplits;
	}

	/**
	 * Get ao table file status from table
	 * 
	 * @param tableOid
	 *            oid of table
	 * @return ao table file status
	 * @throws SQLException
	 */
	public HAWQAOFileStatus[] getAOTableFileAttributes(int tableOid)
			throws SQLException
	{
		ArrayList<HAWQAOFileStatus> fileAttributes = getAOTableFileAttrList(tableOid);
		return fileAttributes.toArray(new HAWQAOFileStatus[fileAttributes
				.size()]);
	}

	/**
	 * GPSQL-1047
	 * 
	 * Get version from database
	 * 
	 * @return version of database
	 * @throws SQLException
	 */
	public String getDatabaseVersion() throws SQLException
	{
		resultSet = statement.executeQuery("SELECT version()");
		resultSet.next();
		return resultSet.getString(1);
	}
}