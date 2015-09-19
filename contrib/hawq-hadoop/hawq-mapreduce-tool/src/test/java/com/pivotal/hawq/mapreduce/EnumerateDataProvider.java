package com.pivotal.hawq.mapreduce;

import java.util.List;

/**
 * A <code>EnumerateDataProvider</code> enumerates all possible combinations
 * of sample column values from dataset until hits the max row number.
 *
 * If there is no duplicate sample values for each column type,
 * <code>EnumerateDataProvider</code> is guaranteed to generate unique rows.
 *
 * NOTE: <code>EnumerateDataProvider</code> is not suitable for generating
 * large number of rows due to SQL length limit.
 */
public class EnumerateDataProvider implements DataProvider {
	private static final int DEFAULT_MAX_ROWS = 100;

	// max number of rows to generate
	private int maxrows;

	public EnumerateDataProvider() {
		this(DEFAULT_MAX_ROWS);
	}

	public EnumerateDataProvider(int maxrows) {
		this.maxrows = maxrows;
	}

	@Override
	public String getInsertSQLs(HAWQTable table) {
		List<String> columnTypes = table.getColumnTypes();

		String[] values = new String[columnTypes.size()];
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO ").append(table.getTableName()).append(" values ");

		genDataRecursive(buf, values, 0, maxrows, 0, columnTypes);

		buf.delete(buf.length() - 2, buf.length()).append(';');
		return buf.toString();
	}

	// Using a recursive approach to enumerate all possible combinations
	// of column values until we hit the max row number.
	// Return number of rows generated.
	private int genDataRecursive(StringBuilder buf,
								 String[] values,
								 int currentColumn,
								 int rowNumMax,
								 int rowNum,
								 List<String> columnTypes) {

		if (currentColumn == columnTypes.size()) {
			buf.append("(").append(values[0]);
			for (int c = 1; c < columnTypes.size(); c++) {
				buf.append(", ").append(values[c]);
			}
			buf.append("),\n");
			return rowNum + 1;
		}

		List<String> cvals = MRFormatConfiguration.DATA_SET.get(columnTypes.get(currentColumn));
		for (String cval : cvals) {
			values[currentColumn] = cval;
			rowNum = genDataRecursive(buf, values, currentColumn+1, rowNumMax, rowNum, columnTypes);
			if (rowNum >= rowNumMax) break;
		}
		return rowNum;
	}
}
