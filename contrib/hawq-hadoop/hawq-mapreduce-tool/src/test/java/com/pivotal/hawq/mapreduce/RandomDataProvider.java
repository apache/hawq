package com.pivotal.hawq.mapreduce;

import java.util.List;
import java.util.Random;

/**
 * A <code>RandomDataProvider</code> that generates fixed number
 * of rows, but randomly picks sample values for each column.
 *
 * There may be duplicate rows in the generated data.
 *
 * NOTE: <code>RandomDataProvider</code> is not suitable for generating
 * large number of rows due to SQL length limit.
 */
public class RandomDataProvider implements DataProvider {
	private static final int DEFAULT_ROW_NUM = 100;

	// number of rows to generate
	private int rownum;

	public RandomDataProvider() {
		this(DEFAULT_ROW_NUM);
	}

	public RandomDataProvider(int rownum) {
		this.rownum = rownum;
	}

	@Override
	public String getInsertSQLs(HAWQTable table) {
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO ").append(table.getTableName()).append(" values ");

		Random random = new Random();
		List<String> columnTypes = table.getColumnTypes();

		for (int i = 0; i < rownum; i++) {
			buf.append("(").append(randomValueForType(columnTypes.get(0), random));
			for (int c = 1; c < columnTypes.size(); c++) {
				buf.append(", ").append(randomValueForType(columnTypes.get(c), random));
			}
			buf.append("),\n");
		}

		buf.delete(buf.length() - 2, buf.length()).append(';');
		return buf.toString();
	}

	private String randomValueForType(String colType, Random random) {
		List<String> cvals = MRFormatConfiguration.DATA_SET.get(colType);
		return cvals.get(random.nextInt(cvals.size()));
	}
}