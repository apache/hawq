package com.pivotal.hawq.mapreduce;

/**
 * A <code>DataProvider</code> provides test data for <code>HAWQTable</code>
 */
public interface DataProvider {

	public static final DataProvider EMPTY = new EmptyDataProvider();
	public static final DataProvider ENUMERATE = new EnumerateDataProvider();
	public static final DataProvider RANDOM = new RandomDataProvider();


	/**
	 * Generate test data for the given HAWQ table, return
	 * insertion SQLs to populate the data.
	 * @param table definition for a HAWQ table
	 * @return insertion SQLs to populate test data for specified table.
	 */
	String getInsertSQLs(HAWQTable table);
}