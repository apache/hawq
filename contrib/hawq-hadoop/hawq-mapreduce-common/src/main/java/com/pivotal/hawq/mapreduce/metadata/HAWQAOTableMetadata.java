package com.pivotal.hawq.mapreduce.metadata;

import com.pivotal.hawq.mapreduce.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;

/**
 * Represent metadata for HAWQ append only row orientation table.
 */
public class HAWQAOTableMetadata {
	private HAWQDatabaseMetadata dbMetadata;
	private HAWQSchema schema;
	private HAWQAOFileStatus[] fileStatuses;

	public HAWQAOTableMetadata(HAWQDatabaseMetadata dbMetadata,
							   HAWQSchema schema,
							   HAWQAOFileStatus[] fileStatuses) {
		this.dbMetadata = dbMetadata;
		this.schema = schema;
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

	public HAWQSchema getSchema() {
		return schema;
	}

	public HAWQAOFileStatus[] getFileStatuses() {
		return fileStatuses;
	}
}
