package com.pxf.tests.dataprepares.hbase;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.TimeZone;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.pivotal.pxfauto.infra.fileformats.IDataPreparer;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.hbase.HBaseTable;

/**
 * HBaseDataPreparer creates given rows for every numberOfSplits. (rows *
 * numberOfSplits). The data tries to include the various data types:
 * String,UTF8 string, Integer, ASCII, real, float, char, small integer, big
 * integer and Time stamp.
 */
public class HBaseDataPreparer implements IDataPreparer {

	private int numberOfSplits = 1;
	private String rowKeyPrefix = "";
	private String columnFamilyName = "cf1";
	private boolean useNull = false;
	private final int UNSUPORTTED_CHAR = 92;
	private final int FIRST_PRINTABLE_CHAR = 32;
	private final int LAST_PRINTABLE_CHAR = 126;

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		byte[] columnFamily = Bytes.toBytes(columnFamilyName);
		ArrayList<Put> generatedRows = new ArrayList<Put>();

		for (int splitIndex = 0; splitIndex < (numberOfSplits + 1); ++splitIndex) {
			for (int i = 0, chars = FIRST_PRINTABLE_CHAR; i < rows; ++i, chars = nextChar(chars)) {

				// Row Key
				String rowKey = String.format("%s%08d", rowKeyPrefix, i + splitIndex * rows);
				Put newRow = new Put(Bytes.toBytes(rowKey));

				// Qualifier 1. regular ascii string
				if ((!useNull) || (i % 2 == 0))
					addValue(newRow, columnFamily, "q1", String.format("ASCII%08d", i));

				// Qualifier 2. multibyte utf8 string.
				addValue(newRow, columnFamily, "q2", String.format("UTF8_計算機用語_%08d", i).getBytes());

				// Qualifier 3. integer value.
				if ((!useNull) || (i % 3 == 0))
					addValue(newRow, columnFamily, "q3", String.format("%08d", 1 + i + splitIndex * rows));

				// Qualifier 4. regular ascii (for a lookup table redirection)
				addValue(newRow, columnFamily, "q4", String.format("lookup%08d", i * 2));

				// Qualifier 5. real (float)
				addValue(newRow, columnFamily, "q5", String.format("%d.%d", i, i));

				// Qualifier 6. float (double)
				addValue(newRow, columnFamily, "q6", String.format("%d%d%d%d.%d", i, i, i, i, i));

				// Qualifier 7. bpchar (char)
				addValue(newRow, columnFamily, "q7", String.format("%c", chars));

				// Qualifier 8. smallint (short)
				addValue(newRow, columnFamily, "q8", String.format("%d", (i % Short.MAX_VALUE)));

				// Qualifier 9. bigint (long)
				Long value9 = ((i * i * i * 10000000000L + i) % Long.MAX_VALUE) * (long) Math.pow(-1, i % 2);
				addValue(newRow, columnFamily, "q9", value9.toString());

				// Qualifier 10. boolean
				addValue(newRow, columnFamily, "q10", Boolean.toString((i % 2) == 0));

				// Qualifier 11. numeric (string)
				addValue(newRow, columnFamily, "q11", (new Double(Math.pow(10, i))).toString());

				// Qualifier 12. Timestamp
				// Removing system timezone so tests will pass anywhere in the
				// world :)
				int timeZoneOffset = TimeZone.getDefault().getRawOffset();
				addValue(newRow, columnFamily, "q12", (new Timestamp(6000 * i - timeZoneOffset)).toString());

				generatedRows.add(newRow);

			}
		}

		((HBaseTable) dataTable).setRowsToGenerate(generatedRows);

		return null;
	}

	/**
	 * get the next printable char
	 * 
	 * @return
	 */
	private int nextChar(int chars) {

		if (chars == LAST_PRINTABLE_CHAR) {
			return FIRST_PRINTABLE_CHAR - 1;
		}

		chars++;

		if (chars == UNSUPORTTED_CHAR) {
			chars++;
		}

		return chars;
	}

	private void addValue(Put row, byte[] cf, String ql, byte[] value)
	{
		row.add(cf, ql.getBytes(), value);
	}

	private void addValue(Put row, byte[] cf, String ql, String value) throws java.io.UnsupportedEncodingException
	{
		addValue(row, cf, ql, value.getBytes("UTF-8"));
	}

	public int getNumberOfSplits() {
		return numberOfSplits;
	}

	public void setNumberOfSplits(int numberOfSplits) {
		this.numberOfSplits = numberOfSplits;
	}

	public String getRowKeyPrefix() {
		return rowKeyPrefix;
	}

	public void setRowKeyPrefix(String rowKeyPrefix) {
		this.rowKeyPrefix = rowKeyPrefix;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	public boolean isUseNull() {
		return useNull;
	}

	public void setUseNull(boolean useNull) {
		this.useNull = useNull;
	}
}