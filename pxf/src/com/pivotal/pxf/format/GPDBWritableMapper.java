package com.pivotal.pxf.format;

import com.pivotal.pxf.exception.UnsupportedTypeException;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.hadoop.io.GPDBWritable.TypeMismatchException;

/*
 * Class for mapping GPDBWritable get functions to java types.
 */
public class GPDBWritableMapper {

	private GPDBWritable gpdbWritable;
	private int type;
	private DataGetter getter = null;
	
	public GPDBWritableMapper(GPDBWritable gpdbWritable)
	{
		this.gpdbWritable = gpdbWritable;
	}
	
	public static int convertJavaToGPDBType(String type) {
		
		if ((type.compareTo("boolean") == 0) || (type.compareTo("[Z") == 0))
		{
			return GPDBWritable.BOOLEAN;
		}
		else if ((type.compareTo("int") == 0) || (type.compareTo("[I") == 0))
		{
			return GPDBWritable.INTEGER;
		}
		else if ((type.compareTo("double") == 0) || (type.compareTo("[D") == 0))
		{
			return GPDBWritable.FLOAT8;
		}
		else if ((type.compareTo("java.lang.String") == 0) || 
				 (type.compareTo("[Ljava.lang.String;") == 0))
		{
			return GPDBWritable.TEXT;
		}
		else if ((type.compareTo("float") == 0) || (type.compareTo("[F") == 0))
		{
			return GPDBWritable.REAL;

		}
		else if ((type.compareTo("long") == 0) || (type.compareTo("[J") == 0))
		{
			return GPDBWritable.BIGINT;
		}
		else if (type.compareTo("[B") == 0)
		{
			return GPDBWritable.BYTEA;
		}
		else if ((type.compareTo("short") == 0) || (type.compareTo("[S") == 0)) 
		{
			return GPDBWritable.SMALLINT;
		}
		else 
		{
			throw new UnsupportedTypeException("Type " + type + " is not supported by GPDBWritable");
		}
	}
	
	public void setDataType(String javaType) throws UnsupportedTypeException {
		int type = convertJavaToGPDBType(javaType);
		setDataType(type);
	}
	
	public void setDataType(int type) throws UnsupportedTypeException
	{
		this.type = type;
		
		switch (type) {
		case GPDBWritable.BOOLEAN  : 
			getter = new BooleanDataGetter();
			break;
		case GPDBWritable.BYTEA	   : 
			getter = new BytesDataGetter();
			break;
		case GPDBWritable.BIGINT   : 
			getter = new LongDataGetter();
			break;
		case GPDBWritable.SMALLINT : 
			getter = new ShortDataGetter();
			break;
		case GPDBWritable.INTEGER  : 
			getter = new IntDataGetter();
			break;
		case GPDBWritable.TEXT     : 
			getter = new StringDataGetter();
			break;
		case GPDBWritable.REAL     : 
			getter = new FloatDataGetter();
			break;
		case GPDBWritable.FLOAT8   : 
			getter = new DoubleDataGetter();
			break;
		default:
			throw new UnsupportedTypeException(
					"Type " + GPDBWritable.getTypeName(type) + 
					" is not supported by GPDBWritable");
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
		return "getter type = " + GPDBWritable.getTypeName(type);
	}
	
	public static boolean isArray(String javaType)
	{
		return ((javaType.startsWith("[") && (javaType.compareTo("[B") != 0)));
	}
	
}
