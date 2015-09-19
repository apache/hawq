package com.pivotal.hawq.mapreduce.metadata;

import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;

/**
 * Represent metadata for HAWQ Parquet table.
 */
public class HAWQParquetTableMetadata {
	private HAWQDatabaseMetadata dbMetadata;
	private HAWQFileStatus[] fileStatuses;

	public HAWQParquetTableMetadata(HAWQDatabaseMetadata dbMetadata,
									HAWQFileStatus[] fileStatuses) {
		this.dbMetadata = dbMetadata;
		this.fileStatuses = fileStatuses;
	}

	public String getDatabaseVersion() {
		return dbMetadata.getVersion();
	}

	public String getDatabaseEncoding() {
		return dbMetadata.getEncoding();
	}

	public String getDatabaseDfsURL() {
		return dbMetadata.getDfsURL();
	}

	public HAWQFileStatus[] getFileStatuses() {
		return fileStatuses;
	}
}
