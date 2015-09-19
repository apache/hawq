package com.pivotal.hawq.mapreduce.metadata;

/**
 * Thrown to indicate that something goes wrong when extracting table's metadata.
 */
public class MetadataAccessException extends RuntimeException {

	public MetadataAccessException(String message) {
		super(message);
	}

	public MetadataAccessException(String message, Throwable cause) {
		super(message, cause);
	}

	public MetadataAccessException(Throwable cause) {
		super(cause);
	}
}
