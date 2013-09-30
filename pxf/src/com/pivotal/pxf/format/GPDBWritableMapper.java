package com.pivotal.pxf.format;

import com.pivotal.pxf.exception.UnsupportedTypeException;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.hadoop.io.GPDBWritable.TypeMismatchException;

/*
 * Class for mapping GPDBWritable get functions to java types.
 */
public class GPDBWritableMapper {

	private GPDBWritable gpdbWritable;
	private String type;
	private DataGetter getter = null;
	
	public GPDBWritableMapper(GPDBWritable gpdbWritable)
	{
		this.gpdbWritable = gpdbWritable;
	}
	
	public void setDataType(String type) throws UnsupportedTypeException
	{
		this.type = type;

		if ((type.compareTo("boolean") == 0) || (type.compareTo("[Z") == 0))
		{
			getter = new BooleanDataGetter();
		}
		else if ((type.compareTo("int") == 0) || (type.compareTo("[I") == 0))
		{
			getter = new IntDataGetter();
		}
		else if ((type.compareTo("double") == 0) || (type.compareTo("[D") == 0))
		{
			getter = new DoubleDataGetter();
		}
		else if ((type.compareTo("java.lang.String") == 0) || 
				 (type.compareTo("[Ljava.lang.String;") == 0))
		{
			getter = new StringDataGetter();
		}
		else if ((type.compareTo("float") == 0) || (type.compareTo("[F") == 0))
		{
			getter = new FloatDataGetter();
		}
		else if ((type.compareTo("long") == 0) || (type.compareTo("[J") == 0))
		{
			getter = new LongDataGetter();
		}
		else if (type.compareTo("[B") == 0)
		{
			getter = new BytesDataGetter();
		}
		else if ((type.compareTo("short") == 0) || (type.compareTo("[S") == 0)) 
		{
			getter = new ShortDataGetter();
		}
		else 
		{
			throw new UnsupportedTypeException("Type " + type + " is not supported by GPDBWritable");
		}
	}
	
	public Object getData(int colIdx) throws TypeMismatchException
	{		
		return getter.getData(colIdx);
	}
	
	private interface DataGetter {
		
		abstract Object getData(int colIdx) throws TypeMismatchException; 	
	}
	
	private class BooleanDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getBoolean(colIdx);
		}
	}
	
	private class BytesDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getBytes(colIdx);
		}
	}
	
	private class DoubleDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getDouble(colIdx);
		}
	}
	
	private class FloatDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getFloat(colIdx);
		}
	}
	
	private class IntDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getInt(colIdx);
		}
	}
	
	private class LongDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getLong(colIdx);
		}
	}
	
	private class ShortDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getShort(colIdx);
		}
	}
	
	private class StringDataGetter implements DataGetter
	{
		public Object getData(int colIdx) throws TypeMismatchException
		{
			return gpdbWritable.getString(colIdx);
		}
	}
	
	public String toString() 
	{
		return "getter type = " + type;
	}
	
	public static boolean isArray(String type)
	{
		return ((type.startsWith("[") && (type.compareTo("[B") != 0)));
	}
	
}
