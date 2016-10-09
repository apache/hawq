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


import com.pivotal.hawq.mapreduce.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Accessor for reading metadata from a YAML formatted file,
 * which was generated using hawq extract utility.
 */
class MetadataYAMLAccessor extends MetadataAccessor {
	private HAWQTableFormat tableFormat;
	private String tableName;

	private HAWQAOTableMetadata aoMetadata;
	private HAWQParquetTableMetadata parquetMetadata;

	protected MetadataYAMLAccessor(String file) throws FileNotFoundException {
		InputStream input = new FileInputStream(new File(file));
		Yaml yaml = new Yaml();

		Map<?, ?> metadata = (Map<?, ?>) yaml.load(input);

		String dbVersion = metadata.get("DBVersion").toString();
		String dbEncoding = metadata.get("Encoding").toString();
		String dbDfsURL = metadata.get("DFS_URL").toString();
		HAWQDatabaseMetadata dbMetadata = new HAWQDatabaseMetadata(dbVersion, dbEncoding, dbDfsURL);

		tableName = metadata.get("TableName").toString();

		String format = metadata.get("FileFormat").toString();
		tableFormat = HAWQTableFormat.valueOf(format);

		switch (tableFormat) {
			case AO:
				aoMetadata = extractAOMetadata(dbMetadata, metadata);
				break;
			case Parquet:
				parquetMetadata = extractParquetMetadata(dbMetadata, metadata);
				break;
			default:
				throw new UnsupportedOperationException(tableFormat + " table is not supported!");
		}
	}

	//------------------------------------------------------------
	//---- extract AO table info
	//------------------------------------------------------------

	private HAWQAOTableMetadata extractAOMetadata(HAWQDatabaseMetadata dbMetadata, Map<?,?> metadata) {
		HAWQSchema schema = extractAOSchema((List<?>) metadata.get("AO_Schema"));
		HAWQAOFileStatus[] fileStatuses = extractAOFileStatuses((Map<?, ?>) metadata.get("AO_FileLocations"));
		return new HAWQAOTableMetadata(dbMetadata, schema, fileStatuses);
	}

	private HAWQSchema extractAOSchema(List<?> ao_schema) {
		List<HAWQField> hawqFields = new ArrayList<HAWQField>();

		for (int i = 0; i < ao_schema.size(); ++i) {
			Map<?, ?> field = (Map<?, ?>) ao_schema.get(i);
			String fieldName = field.get("name").toString();
			String type = field.get("type").toString();
			HAWQField hawqField = null;
			// TODO move this logic to AOInputFormat
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
					throw new UnsupportedOperationException(
							"unsupported array type " + type + " for field " + fieldName);
				}

			} else {
				hawqField = HAWQSchema.optional_field(
						HAWQPrimitiveField.PrimitiveType.valueOf(type.toUpperCase()),
						fieldName
				);
			}

			assert hawqField != null;
			hawqFields.add(hawqField);
		}

		return new HAWQSchema(tableName, hawqFields);
	}

	private HAWQAOFileStatus[] extractAOFileStatuses(Map<?,?> ao_fileLocations) {
		List<HAWQAOFileStatus> result = new ArrayList<HAWQAOFileStatus>();

		result.addAll(loadAOFileStatuses(ao_fileLocations));

		List<?> partitions = (List<?>) ao_fileLocations.get("Partitions");
		if (partitions != null) {
			for (int i = 0; i < partitions.size(); ++i) {
				result.addAll(loadAOFileStatuses((Map<?, ?>) partitions.get(i)));
			}
		}

		return result.toArray(new HAWQAOFileStatus[result.size()]);
	}

	private List<HAWQAOFileStatus> loadAOFileStatuses(Map<?, ?> fileLocations) {
		List<HAWQAOFileStatus> fileStatuses = new ArrayList<HAWQAOFileStatus>();

		int blockSize = Integer.parseInt(fileLocations.get("Blocksize").toString());
		boolean checksum = Boolean.parseBoolean(fileLocations.get("Checksum").toString());
		String compressType = fileLocations.get("CompressionType") == null ?
				"none" : fileLocations.get("CompressionType").toString().toLowerCase();

		List<?> files = (List<?>) fileLocations.get("Files");

		for (int i = 0; i < files.size(); ++i) {
			Map<?, ?> file = (Map<?, ?>) files.get(i);
			String filePath = file.get("path").toString();
			long fileLen = Long.parseLong(file.get("size").toString());
			if (fileLen == 0) {
			  throw new IllegalStateException("Empty file can not be processed by HAWQAOInputFormat since it is an empty file.");
			}

			fileStatuses.add(new HAWQAOFileStatus(filePath, fileLen, checksum, compressType, blockSize));
		}

		return fileStatuses;
	}

	//------------------------------------------------------------
	//---- extract Parquet table info
	//------------------------------------------------------------

	private HAWQParquetTableMetadata extractParquetMetadata(HAWQDatabaseMetadata dbMetadata, Map<?, ?> metadata) {
		return new HAWQParquetTableMetadata(
				dbMetadata,
				extractParquetFileStatuses((Map<?, ?>) metadata.get("Parquet_FileLocations")));
	}

	private HAWQFileStatus[] extractParquetFileStatuses(Map<?, ?> parquet_fileLocations) {
		List<HAWQFileStatus> result = new ArrayList<HAWQFileStatus>();

		result.addAll(loadParquetFileStatuses(parquet_fileLocations));

		List<?> partitions = (List<?>) parquet_fileLocations.get("Partitions");
		if (partitions != null) {
			for (int i = 0; i < partitions.size(); ++i) {
				result.addAll(loadParquetFileStatuses((Map<?, ?>) partitions.get(i)));
			}
		}

		return result.toArray(new HAWQFileStatus[result.size()]);
	}

	private List<HAWQFileStatus> loadParquetFileStatuses(Map<?,?> fileLocations) {
		List<HAWQFileStatus> fileStatuses = new ArrayList<HAWQFileStatus>();

		List<?> files = (List<?>) fileLocations.get("Files");
		for (int i = 0; i < files.size(); ++i) {
			Map<?, ?> file = (Map<?, ?>) files.get(i);
			final String filePath = file.get("path").toString();
			final long fileLen = Long.parseLong(file.get("size").toString());
			if (fileLen == 0) {
			  throw new IllegalStateException("Empty file can not be processed by HAWQParquetInputFormat since it is an empty file.");
			}
			fileStatuses.add(new HAWQFileStatus(filePath, fileLen));
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
