package com.pivotal.hawq.mapreduce;

/**
 * A dummy provider that provides no data at all.
 */
class EmptyDataProvider implements DataProvider {
	@Override
	public String getInsertSQLs(HAWQTable table) {
		return "";
	}
}
