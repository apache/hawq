package com.pivotal.hawq.mapreduce;

import java.util.List;

/**
 * A <code>SeriesIntDataProvider</code> that provides
 * series int data for table with single int8 column.
 */
public class SeriesIntDataProvider implements DataProvider {
	private static final int DEFAULT_ROW_NUM = 10000;

	private int rownum;

	public SeriesIntDataProvider() {
		this(DEFAULT_ROW_NUM);
	}

	public SeriesIntDataProvider(int rownum) {
		this.rownum = rownum;
	}

	@Override
	public String getInsertSQLs(HAWQTable table) {
		List<String> colTypes = table.getColumnTypes();
		if (colTypes.size() != 1 || !colTypes.get(0).equals("int8"))
			throw new IllegalArgumentException(
					"SeriesIntDataProvider only accepts table with single int8 column");

		return String.format("INSERT INTO %s SELECT * FROM generate_series(1, %s);",
							 table.getTableName(), rownum);
	}
}
