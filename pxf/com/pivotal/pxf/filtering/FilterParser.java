package com.pivotal.pxf.filtering;

import java.util.Stack;
import java.util.Map;
import java.util.HashMap;

/*
 * The parser code which goes over a filter string and pushes operands onto a stack
 * Once an operation is read, the evaluate function is called for the IFilterEvaluator
 * interface with two pop-ed operands.
 *
 * A string of filters looks like this:
 * a2c5o1a1c"abc"o2o7
 * which means column#2 < 5 AND column#1 > "abc"
 *
 * It is a RPN serialized representation of a filters tree in GPDB where
 * a means an attribute (column)
 * c means a constant (either string or numeric)
 * o means operator
 *
 * Assuming all operators are binary, RPN representation allows it to be read
 * left to right easily.
 *
 * FilterParser only knows about columns and constants. The rest is up to the implementor 
 * of IFilterEvaluator.
 *
 * FilterParser makes sure a column objects are always on the left of the 
 * expression (when relevant)
 */
public class FilterParser
{
	private int index;
	private String filterString;
	private Stack<Object> operandsStack;
	private IFilterEvaluator evaluator;
	private Map<Integer, Operation> operatorTranslationMap;

	/*
	 * Operations supported by the parser
	 */
	public enum Operation
	{
		HDOP_LT,
		HDOP_GT,
		HDOP_LE,
		HDOP_GE,
		HDOP_EQ,
		HDOP_NE,
		HDOP_AND
	};

	/*
	 * Interface a user of FilterParser should implement
	 * This is used to let the user evaluate expressions in the manner she 
	 * sees fit
	 *
	 * When an operator is parsed, this function is called to let the user decide
	 * what to do with it operands.
	 */
	interface IFilterEvaluator
	{
		public Object evaluate(Operation operation, Object left, Object right) throws Exception;
	}

	/*
	 * The class represents a column index
	 * It used to know the type of an operand in the stack
	 */
	public class ColumnIndex
	{
		private int index;
		public ColumnIndex(int idx)
		{
			index = idx;
		}

		public int index()
		{
			return index;
		}
	}

	/*
	 * The class represents a constant object (String, Long, ...)
	 * It used to know the type of an operand in the stack
	 */
	public class Constant
	{
		private Object constant;
		public Constant(Object obj)
		{
			constant = obj;
		}

		public Object constant()
		{
			return constant;
		}
	}
	
	/*
	 * Basic filter provided for cases where the target storage system does not provide it's own filter
	 * For example: Hbase storage provides it's own filter but for a Writable based record in a sequence
	 * file there is no filter provided and so we need to have a default
	 */
	static public class BasicFilter
	{
		private Operation oper;
		private ColumnIndex column;
		private Constant constant;
		
		/*
		 * C'tor
		 */
		public BasicFilter(Operation inOper, ColumnIndex inColumn, Constant inConstant)
		{
			oper = inOper;
			column = inColumn;
			constant = inConstant;
		}
		
		/*
		 * returns oper field
		 */
		public Operation getOperation()
		{
			return oper;
		}
		
		/*
		 * returns column field
		 */
		public ColumnIndex getColumn()
		{
			return column;
		}
		
		/*
		 * returns constant field
		 */
		public Constant getConstant()
		{
			return constant;
		}
	}

	/*
	 * Exception that can occur while parsing the string
	 */
	class FilterStringSyntaxException extends Exception
	{
		FilterStringSyntaxException(String desc)
		{
			super(desc);
		}
	}

	public FilterParser(IFilterEvaluator eval)
	{
		operandsStack = new Stack<Object>();
		evaluator = eval;
		initOperatorTransMap();
	}

	public Object parse(String filter) throws Exception
	{
		index = 0;
		filterString = filter;

		while (index < filterString.length())
		{
			char op = filterString.charAt(index);
			++index; // skip op character
			switch(op)
			{
				case 'a':
					operandsStack.push(new ColumnIndex(safeToInt(parseNumber())));
					break;
				case 'c':
					operandsStack.push(new Constant(parseParameter()));
					break;
				case 'o':
					// Parse and translate opcode
					Operation operation = operatorTranslationMap.get(safeToInt(parseNumber()));

					// Pop right operand
					if (operandsStack.empty())
						throw new FilterStringSyntaxException("missing operands for op " + operation + " at " + index);
					Object rightOperand = operandsStack.pop();

					// Pop left operand
					if (operandsStack.empty())
						throw new FilterStringSyntaxException("missing operands for op " + operation + " at " + index);
					Object leftOperand = operandsStack.pop();

					// Normalize order, evaluate
					// Column should be on the left
					Object result;
					if (leftOperand instanceof Constant) // column on the right, reverse expression
						result = evaluator.evaluate(reverseOp(operation), rightOperand, leftOperand);
					else // no swap, column on the left
						result = evaluator.evaluate(operation, leftOperand, rightOperand);

					// Store result on stack
					operandsStack.push(result);
					break;
				default:
					throw new FilterStringSyntaxException("unknown opcode " + op + 
														  "(" + (int)op + ") at " + index);
			}
		}

		if (operandsStack.empty())
			throw new FilterStringSyntaxException("filter parsing ended with no result");

		Object result = operandsStack.pop();

		if (!operandsStack.empty())
			throw new FilterStringSyntaxException("Stack not empty, missing operators?");

		return result;
	}

	int safeToInt(Long value) throws FilterStringSyntaxException
	{
		if (value > Integer.MAX_VALUE)
			throw new FilterStringSyntaxException("value " + value + " larger than intmax at " + index);

		return value.intValue();
	}

	/*
	 * Parses either a number or a string
	 */
	private Object parseParameter() throws Exception
	{
		if (index == filterString.length())
			throw new FilterStringSyntaxException("argument should follow at " + index);

		if (senseString())
			return parseString();

		return parseNumber();
	}

	private boolean senseString()
	{
		return filterString.charAt(index) == '"';
	}

	/*
	 * Parses a - or a +
	 * True if -, false o/w
	 * Increments index if either +/-
	 */
	private boolean parseSign()
	{
		int chr = filterString.charAt(index);

		if (chr != '-' &&
			chr != '+')
			return false;

		++index;
		return (chr == '-');
	}

	private Long parseNumber() throws Exception
	{
		if (index == filterString.length())
			throw new FilterStringSyntaxException("numeric argument expected at " + index);

		boolean negative = parseSign();
		Long number = parseDigits();

		if (negative)
			number *= -1L;

		return number;
	}

	/*
	 * Parses the longest sequence of digits into a number
	 * advances the index accordingly
	 */
	private Long parseDigits() throws Exception
	{
		Long result = 0L;
		int i = 0;
		for (i = index; i < filterString.length(); ++i)
		{
			int chr = filterString.charAt(i);
			if (chr < '0' || chr > '9')
				break;

			result = result * 10L;
			result = result + (chr - '0');
		}

		if (i == index)
			throw new FilterStringSyntaxException("numeric argument expected at " + index);

		index = i;
		return result;
	}

	/*
	 * Parses a string after its beginning '"' until its ending '"'
	 * advances the index accordingly
	 *
	 * Currently the string cannot contain '"' itself
	 * TODO add support for '"' inside the string
	 */
	private String parseString() throws Exception
	{
		String result = "";
		boolean ended = false;
		int i = 0;

		// starting from index + 1 to skip leading "
		for (i = index + 1; i < filterString.length(); ++i)
		{
			char chr = filterString.charAt(i);
			if (chr == '"')
			{
				ended = true;
				break;
			}
			result += chr;
		}

		if (!ended)
			throw new FilterStringSyntaxException("string started at " + index + " not ended with \"");

		index = i + 1; // +1 to skip ending "
		return result;
	}

	/*
	 * The function takes an operator and reverses it
	 * e.g. > turns into <
	 */
	private Operation reverseOp(Operation operation)
	{
		switch (operation)
		{
			case HDOP_LT:
				operation = Operation.HDOP_GT;
				break;
			case HDOP_GT:
				operation = Operation.HDOP_LT;
				break;
			case HDOP_LE:
				operation = Operation.HDOP_GE;
				break;
			case HDOP_GE:
				operation = Operation.HDOP_LE;
				break;
			default:
				// no change o/w
		}

		return operation;
	}

	/*
	 * Create a translation table of opcodes to their enum meaning
	 *
	 * These codes corresponds the codes in GPDB C code
	 * see gphdfilters.h in pxf protocol
	 */
	private void initOperatorTransMap()
	{
		operatorTranslationMap = new HashMap<Integer, Operation>();
		operatorTranslationMap.put(1, Operation.HDOP_LT);
		operatorTranslationMap.put(2, Operation.HDOP_GT);
		operatorTranslationMap.put(3, Operation.HDOP_LE);
		operatorTranslationMap.put(4, Operation.HDOP_GE);
		operatorTranslationMap.put(5, Operation.HDOP_EQ);
		operatorTranslationMap.put(6, Operation.HDOP_NE);
		operatorTranslationMap.put(7, Operation.HDOP_AND);
	}
}
