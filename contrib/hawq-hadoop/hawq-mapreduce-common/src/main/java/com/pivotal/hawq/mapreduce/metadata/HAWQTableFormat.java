package com.pivotal.hawq.mapreduce.metadata;

/**
 * Different storage format for table in HAWQ.
 */
public enum HAWQTableFormat {
	AO {
		public String getOrientation() { return "row"; }
	},
	CO {
		public String getOrientation() { return "column"; }
	},
	Parquet {
		public String getOrientation() { return "parquet"; }
	},
	Other;

	public String getOrientation() {
		throw new UnsupportedOperationException();
	}
}
