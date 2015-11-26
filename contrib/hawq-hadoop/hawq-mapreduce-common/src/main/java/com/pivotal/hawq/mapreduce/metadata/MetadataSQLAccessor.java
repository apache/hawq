package com.pivotal.hawq.mapreduce.metadata;

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
import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.util.HAWQJdbcUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Accessor for extracting table metadata by querying catalog using JDBC.
 *
 * Extracting will be done at construction phase.
 */
class MetadataSQLAccessor extends MetadataAccessor {
	private Connection conn;

	private int 	dbId;
	private int 	dbTblSpcId;
	private HAWQDatabaseMetadata dbMetadata;

	/**
	 *  DFS location (exclude DFS_PREFIX) for each segments, indexed by contentid.
	 *  eg: ['/gpseg0', '/gpseg1']
	 */
	private String segmentLocation;

	private HAWQTableFormat tableFormat;
	private HAWQAOTableMetadata aoMetadata;
	private HAWQParquetTableMetadata parquetMetadata;


	private static class RelationInfo {
		String 			relname;
		int 			relid;
		int 			relfilenode;
		HAWQTableFormat relformat;

		List<RelationInfo> partitions;

		private RelationInfo(Map<String, String> relPGClass) {
			this.relname		= relPGClass.get("relname");
			this.relid			= Integer.parseInt(relPGClass.get("oid"));
			this.relfilenode	= Integer.parseInt(relPGClass.get("relfilenode"));
			this.relformat		= getStorageFormat(relPGClass.get("relstorage"));
			this.partitions		= Lists.newArrayList();

			if (relformat != HAWQTableFormat.AO && relformat != HAWQTableFormat.Parquet) {
				throw new IllegalArgumentException(String.format(
						"Unsupported storage format for table '%s', only AO and Parquet are supported.",
						relname));
			}
 		}

		/**
		 * Fetch relation information about the given table. Only AO or Parquet table are supported.
		 * One-level partition table are also supported if the parent table and all subpartition table
		 * have the same storage format, eg: both AO or both Parquet.
		 * @param conn		database connection to use
		 * @param tableName name of an AO/Parquet table, valid format are 'namespace_name.table_name' or
		 *                  simply 'table_name' if use the default 'public' namespace
		 * @return table's information
		 * @throws IllegalArgumentException if `tableName` is not a valid table name
		 * @throws SQLException if error happens when querying database.
		 */
		public static RelationInfo newInstanceFromDB(
				Connection conn, String tableName) throws SQLException {

			String relnamespace = "public";
			String relname;

			String[] nameParts = tableName.split("\\.");
			if (nameParts.length == 1) {
				relname = nameParts[0];

			} else if (nameParts.length == 2) {
				relnamespace = nameParts[0];
				relname = nameParts[1];

			} else {
				throw new IllegalArgumentException("not a valid table name: " + tableName);
			}

			// get pg_class information for the target relation itself
			Map<String, String> relPGClass = getPGClass(conn, relnamespace, relname);
			if (relPGClass == null)
				throw new IllegalArgumentException("table '" + tableName + "' does not exist!");

			RelationInfo rel = new RelationInfo(relPGClass);

			// get pg_class information for all the possible subpartition tables
			for (Map<String, String> p : getPartitions(conn, relnamespace, relname)) {
				if (p.get("parentpartitiontablename") != null) {
					throw new IllegalArgumentException("multi-level partition table is not supported!");
				}

				relnamespace = p.get("partitionschemaname");
				relname = p.get("partitiontablename");

				RelationInfo partRel = new RelationInfo(getPGClass(conn, relnamespace, relname));
				if (partRel.relformat != rel.relformat) {
					throw new IllegalArgumentException(
							String.format("partition table storage format inconsistent: %s is %s, but %s is %s",
									rel.relname, rel.relformat, partRel.relname, partRel.relformat));
				}

				rel.partitions.add(partRel);
			}

			return rel;
		}

		private static Map<String, String> getPGClass(
				Connection conn, String relnamespace, String relname) throws SQLException {
			String sql = String.format(
					"SELECT c.oid, c.*\n" +
					"FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid\n" +
					"WHERE n.nspname='%s' and c.relname='%s' and relkind='r'",
					relnamespace, relname);
			return HAWQJdbcUtils.executeSafeQueryForSingleRow(conn, sql);
		}

		private static HAWQTableFormat getStorageFormat(String relstorage) {
			assert relstorage.length() == 1;
			switch (relstorage.charAt(0)) {
				case 'a':
					return HAWQTableFormat.AO;
				case 'c':
					return HAWQTableFormat.CO;
				case 'p':
					return HAWQTableFormat.Parquet;
				default:
					return HAWQTableFormat.Other;
			}
		}

		private static List<Map<String, String>> getPartitions(
				Connection conn, String relnamespace, String relname) throws SQLException {
			String sql = String.format(
					"SELECT partitionschemaname, partitiontablename, partitionname,\n" +
					"       partitiontype, parentpartitiontablename\n" +
					"FROM pg_partitions\n" +
					"WHERE schemaname='%s' and tablename='%s'",
					relnamespace, relname);
			return HAWQJdbcUtils.executeSafeQuery(conn, sql);
		}
	}

	protected MetadataSQLAccessor(Connection conn, String tableName) throws SQLException {
		this.conn = conn;
		this.disableORCA();
		this.extractGeneralDBInfo();

		RelationInfo relation = RelationInfo.newInstanceFromDB(conn, tableName.toLowerCase());
		this.tableFormat = relation.relformat;

		switch (tableFormat) {
			case AO:
				aoMetadata = extractAOMetadata(relation);
				break;
			case Parquet:
				parquetMetadata = extractParquetMetadata(relation);
				break;
			default:
				throw new AssertionError(tableFormat);
		}
	}

	private void disableORCA() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("set optimizer=off;");

		} finally {
			HAWQJdbcUtils.free(stmt, null);
		}
	}

	private void extractGeneralDBInfo() throws SQLException {
		String sql = "SELECT oid, dat2tablespace, pg_encoding_to_char(encoding) encoding\n" +
				"FROM pg_database WHERE datname=current_database()";
		Map<String, String> res = HAWQJdbcUtils.executeSafeQueryForSingleRow(conn, sql);

		dbId 		= Integer.parseInt(res.get("oid"));
		dbTblSpcId	= Integer.parseInt(res.get("dat2tablespace"));
		String encoding	= res.get("encoding");

		sql = "select version() as version";
		res = HAWQJdbcUtils.executeSafeQueryForSingleRow(conn, sql);
		String version	= res.get("version");

		sql = "SELECT fselocation\n" +
				"FROM pg_filespace_entry\n" +
				"JOIN pg_filespace fs ON fsefsoid=fs.oid\n" +
				"WHERE fsname='dfs_system';";

		List<Map<String, String>> rows = HAWQJdbcUtils.executeSafeQuery(conn, sql);

		if (rows == null || rows.size() != 1)
		{
			HAWQException e = new HAWQException("Could NOT find the appropriate filespace entry", 0);
			throw new AssertionError("failed to get table data file path prefix!", e);
		}
		
		String dfsURL = null;
	
		Map<String, String> row = rows.get(0);
		try {
			URI uri = new URI(row.get("fselocation"));
			dfsURL = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
			this.segmentLocation = uri.getPath();
		} catch(URISyntaxException e) {
			throw new AssertionError("failed to parse segment locations!", e);
		}
		
		dbMetadata = new HAWQDatabaseMetadata(version, encoding, dfsURL);
	}

	//------------------------------------------------------------
	//---- extract AO table info
	//------------------------------------------------------------

	private HAWQAOTableMetadata extractAOMetadata(RelationInfo relation) throws SQLException {
		HAWQSchema schema = extractAOSchema(relation);
		HAWQAOFileStatus[] fileStatuses = extractAOFileStatuses(relation);
		return new HAWQAOTableMetadata(dbMetadata, schema, fileStatuses);
	}

	private HAWQSchema extractAOSchema(RelationInfo rel) throws SQLException {
		String sql = String.format(
				"SELECT attname as name, typname as type\n" +
						"FROM pg_attribute a join pg_type t on a.atttypid = t.oid\n" +
						"WHERE attrelid=%d and attnum > 0\n" +
						"ORDER BY attnum asc;", rel.relid);
		List<Map<String, String>> rows = HAWQJdbcUtils.executeSafeQuery(conn, sql);
		List<HAWQField> fields = Lists.newArrayList();

		for (Map<String, String> row : rows) {
			String fieldName = row.get("name");
			String type = row.get("type");
			HAWQField hawqField;
			if (type.startsWith("_")) {
				// supported array type
				if (type.equals("_int4")
						|| type.equals("_int8")
						|| type.equals("_int2")
						|| type.equals("_float4")
						|| type.equals("_float8")
						|| type.equals("_bool")
						|| type.equals("_time")
						|| type.equals("_date")
						|| type.equals("_interval")) {

					hawqField = HAWQSchema.optional_field_array(
							HAWQPrimitiveField.PrimitiveType.valueOf(type.substring(1).toUpperCase()),
							fieldName
					);

				} else {
					throw new MetadataAccessException(
							"unsupported array type " + type + " for field " + fieldName);
				}

			} else {
				hawqField = HAWQSchema.optional_field(
						HAWQPrimitiveField.PrimitiveType.valueOf(type.toUpperCase()),
						fieldName
				);
			}

			fields.add(hawqField);
		}

		return new HAWQSchema(rel.relname, fields);
	}

	private HAWQAOFileStatus[] extractAOFileStatuses(
			RelationInfo relation) throws SQLException {
		List<HAWQAOFileStatus> fileStatuses = Lists.newArrayList();

		fileStatuses.addAll(loadAOFileStatuses(relation.relid, relation.relfilenode));
		for (RelationInfo partRel : relation.partitions) {
			fileStatuses.addAll(loadAOFileStatuses(partRel.relid, partRel.relfilenode));
		}

		return fileStatuses.toArray(new HAWQAOFileStatus[fileStatuses.size()]);
	}

	private List<HAWQAOFileStatus> loadAOFileStatuses(
			int relid, int relfilenode) throws SQLException {
		List<HAWQAOFileStatus> fileStatuses = Lists.newArrayList();

		String sql = "SELECT blocksize, compresslevel, checksum, compresstype\n" +
				"FROM pg_appendonly WHERE relid=" + relid;
		Map<String, String> relAppendonly = HAWQJdbcUtils.executeSafeQueryForSingleRow(conn, sql);

		int blockSize = Integer.parseInt(relAppendonly.get("blocksize"));
		String compressType = relAppendonly.get("compresstype");
		if (compressType == null) {
			compressType = "none";
		}
		boolean checksum = relAppendonly.get("checksum").equals("t");

		sql = String.format(
				"SELECT segno as fileno, eof as filesize\n" +
				"FROM pg_aoseg.pg_aoseg_%d\n" +
				"ORDER by fileno;", relid);
		List<Map<String, String>> rows = HAWQJdbcUtils.executeSafeQuery(conn, sql);

		for (Map<String, String> row : rows) {
			String filePath = String.format("%s/%d/%d/%d/%d",
					segmentLocation,
					dbTblSpcId,
					dbId,
					relfilenode,
					Integer.parseInt(row.get("fileno")));

			long fileSize = Long.parseLong(row.get("filesize"));
			fileStatuses.add(new HAWQAOFileStatus(filePath, fileSize, checksum, compressType, blockSize));
		}

		return fileStatuses;
	}

	//------------------------------------------------------------
	//---- extract Parquet table info
	//------------------------------------------------------------

	private HAWQParquetTableMetadata extractParquetMetadata(
			RelationInfo relation) throws SQLException {
		List<HAWQFileStatus> fileStatuses = Lists.newArrayList();

		fileStatuses.addAll(extractParquetFileStatuses(relation.relid, relation.relfilenode));
		for (RelationInfo partRel : relation.partitions) {
			fileStatuses.addAll(extractParquetFileStatuses(partRel.relid, partRel.relfilenode));
		}

		return new HAWQParquetTableMetadata(
				dbMetadata, fileStatuses.toArray(new HAWQFileStatus[fileStatuses.size()]));
	}

	private List<HAWQFileStatus> extractParquetFileStatuses(
			int oid, int relfilenode) throws SQLException {

		List<HAWQFileStatus> fileStatuses = Lists.newArrayList();

		String sql = String.format(
				"SELECT segno as fileno, eof as filesize\n" +
				"FROM pg_aoseg.pg_paqseg_%d\n" +
				"ORDER by fileno;", oid);
		List<Map<String, String>> rows = HAWQJdbcUtils.executeSafeQuery(conn, sql);

		for (Map<String, String> row : rows) {
			String filePath = String.format("%s/%d/%d/%d/%d",
					segmentLocation,
					dbTblSpcId,
					dbId,
					relfilenode,
					Integer.parseInt(row.get("fileno")));

			fileStatuses.add(new HAWQFileStatus(filePath, Long.parseLong(row.get("filesize"))));
		}

		return fileStatuses;
	}

	@Override
	public HAWQTableFormat getTableFormat() {
		return tableFormat;
	}

	@Override
	public HAWQAOTableMetadata getAOMetadata() {
		if (tableFormat != HAWQTableFormat.AO)
			throw new IllegalStateException("shouldn't call getAOMetadata on a " + tableFormat + " table!");
		return aoMetadata;
	}

	@Override
	public HAWQParquetTableMetadata getParquetMetadata() {
		if (tableFormat != HAWQTableFormat.Parquet)
			throw new IllegalStateException("shouldn't call getParquetMetadata on a " + tableFormat + " table!");
		return parquetMetadata;
	}
}
