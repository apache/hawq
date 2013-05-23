package com.pivotal.pxf.filtering;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HConstants;

import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.hbase.IntegerComparator;
import com.pivotal.pxf.utilities.HBaseColumnDescriptor;
import com.pivotal.pxf.utilities.HBaseMetaData;

import java.util.Map;
import java.util.HashMap;

/*
 * This is the implementation of IFilterEvaluator for HBase.
 *
 * The class uses the filter parser code to build a filter object,
 * either simple (single Filter class) or a compound (FilterList)
 * for HBaseAccessor to use for its scan
 *
 * This is done before the scan starts
 * It is not a scan time operation
 *
 * HBase row key column is a special case
 * If the user defined row key column as TEXT and used <,>,<=,>=,= operators
 * the startkey (>/>=) and the endkey (</<=) are stored in addition to 
 * the created filter.
 *
 * This is an addition on top of regular filters and does not replace 
 * any logic in HBase filter objects
 */
public class HBaseFilterEvaluator implements FilterParser.IFilterEvaluator
{
	private HBaseMetaData conf;
	private Map<FilterParser.Operation,CompareFilter.CompareOp> operatorsMap;
	private byte[] startKey;
	private byte[] endKey;

	public HBaseFilterEvaluator(HBaseMetaData configuration)
	{
		conf = configuration;
		initOperatorsMap();
		startKey = HConstants.EMPTY_START_ROW;
		endKey = HConstants.EMPTY_END_ROW;
	}

	/*
	 * Translates a filterString into a HBase Filter object
	 */
	public Filter getFilterObject(String filterString) throws Exception
	{
		FilterParser parser = new FilterParser(this);
		Object result = parser.parse(filterString);

		if (!(result instanceof Filter))
			throw new Exception("String " + filterString + " resolved to no filter");

		return (Filter)result;
	}

	/*
	 * Returns the startKey defined by the user
	 * if the user specified a > / >= operation
	 * on a textual row key column
	 * o/w, start of table
	 */
	public byte[] startKey()
	{
		return startKey;
	}

	/*
	 * Returns the endKey defined by the user
	 * if the user specified a < / <= operation
	 * on a textual row key column
	 * o/w, end of table
	 */
	public byte[] endKey()
	{
		return endKey;
	}

	/*
	 * Implemented for IFilterEvaluator interface
	 * 
	 * Called each time the parser comes across an operator.
	 */
	public Object evaluate(FilterParser.Operation opId,
						   Object leftOperand,
						   Object rightOperand) throws Exception
	{
		if (leftOperand instanceof Filter)
		{
			if (opId != FilterParser.Operation.HDOP_AND ||
				!(rightOperand instanceof Filter))
				throw new Exception("Only AND is allowed between compound expressions");

			return handleCompoundOperations((Filter)leftOperand, (Filter)rightOperand);
		}

		if (!(rightOperand instanceof FilterParser.Constant))
			throw new Exception("expressions of column-op-column are not supported");

		// Assume column is on the left
		return handleSimpleOperations(opId,
									  (FilterParser.ColumnIndex)leftOperand,
									  (FilterParser.Constant)rightOperand);
	}

	/*
	 * Initialize the operatorsMap with appropriate values
	 */
	private void initOperatorsMap()
	{
		operatorsMap = new HashMap<FilterParser.Operation,CompareFilter.CompareOp>();
		operatorsMap.put(FilterParser.Operation.HDOP_LT, CompareFilter.CompareOp.LESS); // "<"
		operatorsMap.put(FilterParser.Operation.HDOP_GT, CompareFilter.CompareOp.GREATER); // ">"
		operatorsMap.put(FilterParser.Operation.HDOP_LE, CompareFilter.CompareOp.LESS_OR_EQUAL); // "<="
		operatorsMap.put(FilterParser.Operation.HDOP_GE, CompareFilter.CompareOp.GREATER_OR_EQUAL); // ">="
		operatorsMap.put(FilterParser.Operation.HDOP_EQ, CompareFilter.CompareOp.EQUAL); // "="
		operatorsMap.put(FilterParser.Operation.HDOP_NE, CompareFilter.CompareOp.NOT_EQUAL); // "!="
	}

	/*
	 * Handles simple column-operator-constant expressions
	 * Creates a special filter in the case the column is the row key column
	 */
	private Filter handleSimpleOperations(FilterParser.Operation opId,
										  FilterParser.ColumnIndex column,
										  FilterParser.Constant constant) throws Exception
	{
		HBaseColumnDescriptor hbaseColumn = (HBaseColumnDescriptor)conf.getColumn(column.index());
		WritableByteArrayComparable comparator = getComparator(hbaseColumn.columnType(),
															   constant.constant());

		// If row key is of type TEXT, allow filter in start/stop row key API in 
		// HBaseAccessor/Scan object
		if (textualRowKey(hbaseColumn))
			storeStartEndKeys(opId, constant.constant());

		if (hbaseColumn.isRowColumn())
			return new RowFilter(operatorsMap.get(opId), comparator);

		return new SingleColumnValueFilter(hbaseColumn.columnFamilyBytes(),
											hbaseColumn.qualifierBytes(),
											operatorsMap.get(opId),
											comparator);
	}

	/*
	 * Resolve the column's type to a comparator class to be used
	 * Currently, we use an Int and a Text
	 */
	private WritableByteArrayComparable getComparator(int type, Object data) throws Exception
	{
		WritableByteArrayComparable result;
		switch(type)
		{
			case GPDBWritable.TEXT:
				result = new BinaryComparator(Bytes.toBytes((String)data));
				break;
			case GPDBWritable.SMALLINT:
			case GPDBWritable.INTEGER:
			case GPDBWritable.BIGINT:
				result = new IntegerComparator((Long)data);
				break;
			default:
				throw new Exception("unsupported column type for filtering " + type);
		}

		return result;
	}

	/* 
	 * Handle AND of already calculated expressions
	 * Currently only AND, in the future OR can be added
	 *
	 * Four cases here: 
	 * 1) both are simple filters
	 * 2) left is a FilterList and right is a filter
	 * 3) left is a filter and right is a FilterList
	 * 4) both are FilterLists
	 *
	 * Currently, 1, 2 can occur, since no parenthesis are used
	 */
	private Filter handleCompoundOperations(Filter left, Filter right)
	{
		FilterList result;

		if (left instanceof FilterList)
		{
			result = (FilterList)left;
			result.addFilter(right);

			return result;
		}

		result = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		// Adding one by one as the documented c'tor taking var args
		// doesn't exist(!!)
		result.addFilter(left);
		result.addFilter(right);

		return result;
	}

	/*
	 * True, if column is of type TEXT and is a row key column
	 */
	private boolean textualRowKey(HBaseColumnDescriptor column)
	{
		return column.isRowColumn() &&
			   column.columnType() == GPDBWritable.TEXT;
	}

	/* 
	 * Sets startKey/endKey and their inclusiveness 
	 * according to the operation op
	 *
	 * TODO allow only one assignment to start/end key
	 * currently, multiple calls to this function might change 
	 * previous assignments
	 */
	private void storeStartEndKeys(FilterParser.Operation op, Object data)
	{
		String key = (String)data;

		// Adding a zero byte to endkey, makes it inclusive
		// Adding a zero byte to startkey, makes it exclusive
		byte[] zeroByte = new byte[1];
		zeroByte[0] = 0;

		switch (op)
		{
			case HDOP_LT:
				endKey = Bytes.toBytes(key);
				break;
			case HDOP_GT:
				startKey = Bytes.add(Bytes.toBytes(key), zeroByte);
				break;
			case HDOP_LE:
				endKey = Bytes.add(Bytes.toBytes(key), zeroByte);
				break;
			case HDOP_GE:
				startKey = Bytes.toBytes(key);
				break;
			case HDOP_EQ:
				startKey = Bytes.toBytes(key);
				endKey = Bytes.add(Bytes.toBytes(key), zeroByte);
				break;
		}
	}
}
