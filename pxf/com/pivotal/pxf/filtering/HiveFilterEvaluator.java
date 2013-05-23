package com.pivotal.pxf.filtering;

import com.pivotal.pxf.utilities.HDFSMetaData;

import java.util.List;
import java.util.LinkedList;

/*
 * This is the implementation of IFilterEvaluator for Hive.
 *
 * The class uses the filter parser code to build a filter object,
 * either simple (single BasicFilter class) or a compound (List<BasicFilter>)
 * for HiveAccessor to use for partition filtering
 */
public class HiveFilterEvaluator implements FilterParser.IFilterEvaluator
{
	private HDFSMetaData conf;

	public HiveFilterEvaluator(HDFSMetaData configuration)
	{
		conf = configuration;
	}

	/*
	 * Translates a filterString into a FilterParser.BasicFilter or a list of such filters
	 */
	public Object getFilterObject(String filterString) throws Exception
	{
		FilterParser parser = new FilterParser(this);
		Object result = parser.parse(filterString);

		if (!(result instanceof FilterParser.BasicFilter) && !(result instanceof List))
			throw new Exception("String " + filterString + " resolved to no filter");

		return result;
	}

	/*
	 * Implemented for IFilterEvaluator interface
	 * 
	 * Called each time the parser comes across an operator.
	 */
	 @SuppressWarnings("unchecked")
	public Object evaluate(FilterParser.Operation opId,
						   Object leftOperand,
						   Object rightOperand) throws Exception
	{
		if (leftOperand instanceof FilterParser.BasicFilter)
		{
			if (opId != FilterParser.Operation.HDOP_AND ||
				!(rightOperand instanceof FilterParser.BasicFilter))
				throw new Exception("Only AND is allowed between compound expressions");
			
			if (leftOperand instanceof List)
				return handleCompoundOperations((List<FilterParser.BasicFilter>)leftOperand, (FilterParser.BasicFilter)rightOperand);
			else 
				return handleCompoundOperations((FilterParser.BasicFilter)leftOperand, (FilterParser.BasicFilter)rightOperand);
		}

		if (!(rightOperand instanceof FilterParser.Constant))
			throw new Exception("expressions of column-op-column are not supported");

		// Assume column is on the left
		return handleSimpleOperations(opId,
									  (FilterParser.ColumnIndex)leftOperand,
									  (FilterParser.Constant)rightOperand);
	}

	/*
	 * Handles simple column-operator-constant expressions
	 * Creates a special filter in the case the column is the row key column
	 */
	private FilterParser.BasicFilter handleSimpleOperations(FilterParser.Operation opId,
															FilterParser.ColumnIndex column,
															FilterParser.Constant constant)
	{
		return new FilterParser.BasicFilter(opId, column, constant);
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
	private  List handleCompoundOperations(List<FilterParser.BasicFilter> left, FilterParser.BasicFilter right)
	{
		left.add(right);
		return left;
	}
	
	private  List handleCompoundOperations(FilterParser.BasicFilter left, FilterParser.BasicFilter right)
	{
		List<FilterParser.BasicFilter> result = new LinkedList<FilterParser.BasicFilter>();
		
		result.add(left);
		result.add(right);

		return result;
	}
}
