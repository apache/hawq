package com.pivotal.hawq.mapreduce.metadata;

/**
 * Represent common information about a HAWQ array.
 */
class HAWQDatabaseMetadata {
	protected String version;
	protected String encoding;
	protected String dfsURL;

	public HAWQDatabaseMetadata(String version, String encoding, String dfsURL) {
		this.version = version;
		this.encoding = encoding;
		this.dfsURL = dfsURL;
	}

	public String getVersion() {
		return version;
	}

	public String getEncoding() {
		return encoding;
	}

	public String getDfsURL() {
		return dfsURL;
	}
}
