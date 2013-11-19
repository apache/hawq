package com.pivotal.pxf.resolvers;

import java.io.DataInputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.exception.UnsupportedTypeException;
import com.pivotal.pxf.format.GPDBWritableMapper;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.hadoop.io.GPDBWritable.TypeMismatchException;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;
import com.pivotal.pxf.utilities.RecordkeyAdapter;

/*
 * Class WritableResolver handles serialization and deserialization of records that were 
 * serialized using Hadoop's Writable serialization framework.
 * 
 * A field named 'recordkey' is treated as a key of the given row, and not as 
 * part of the data schema. @See RecordkeyAdapter.
 * 
 * WritableResolver implements IReadResolver and IWriteResolver interfaces.
 */
public class WritableResolver extends Plugin implements IReadResolver, IWriteResolver
{
	private final static int RECORDKEY_UNDEFINED = -1;
	
	private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();
	private GPDBWritable gpdbWritable;
	private GPDBWritableMapper gpdbWritableMapper;
	
	// reflection fields
	private Object userObject = null;
	private Method[] methods = null;
	private Field[] fields = null;
	private int index_of_readFields = 0;
	////////////////////////////
	
	private Log Log;

	public WritableResolver(InputData input) throws Exception
	{
		super(input);
		gpdbWritable = new GPDBWritable();
		Log = LogFactory.getLog(WritableResolver.class);

		InitInputObject();		
	}

	/*
	 * getFields returns a list of the fields of one record.
	 * Each record field is represented by a OneField item.
	 * OneField item contains two fields: an integer representing the field type and a Java
	 * Object representing the field value.
	 */
	public List<OneField> getFields(OneRow onerow) throws Exception
	{
		userObject = onerow.getData();
		List<OneField> record =  new LinkedList<OneField>();
		int recordkeyIndex = (inputData.getRecordkeyColumn() == null) ? RECORDKEY_UNDEFINED :
			inputData.getRecordkeyColumn().columnIndex();
		int currentIdx = 0;

		for (Field field : fields)
		{
			if (currentIdx == recordkeyIndex)
				currentIdx += recordkeyAdapter.appendRecordkeyField(record, inputData, onerow);
				
			currentIdx += populateRecord(record, field); 
		}

		return record;
	}

	void addOneFieldToRecord(List<OneField> record, int gpdbWritableType, Object val)
	{
		OneField oneField = new OneField();

		oneField.type = gpdbWritableType;
		oneField.val = val;
		record.add(oneField);
	}

	int SetArrayField(List<OneField> record, int gpdbWritableType, Field reflectedField) throws IllegalAccessException
	{
		Object	array = reflectedField.get(userObject);
		int length = Array.getLength(array);

		for (int j = 0; j < length; j++)
		{
			addOneFieldToRecord(record, gpdbWritableType, Array.get(array, j));
		}
		
		return length;
	}

	int populateRecord(List<OneField> record, Field field) throws BadRecordException
	{		
		String javaType = field.getType().getName();
		int	ret = 1;	

		try
		{
            if (javaType.compareTo("boolean") == 0)
            {
                addOneFieldToRecord(record, GPDBWritable.BOOLEAN, field.get(userObject));
            }
			else if (javaType.compareTo("[Z") == 0)
			{
				ret = SetArrayField(record, GPDBWritable.BOOLEAN, field);
			}
			else if (javaType.compareTo("int") == 0)
			{
				addOneFieldToRecord(record, GPDBWritable.INTEGER, field.get(userObject));
			}
			else if (javaType.compareTo("[I") == 0)
			{
				ret = SetArrayField(record, GPDBWritable.INTEGER, field);
			}
			else if (javaType.compareTo("double") == 0)
			{
				addOneFieldToRecord(record, GPDBWritable.FLOAT8, field.get(userObject));
			}
			else if (javaType.compareTo("[D") == 0)
			{
				ret = SetArrayField(record, GPDBWritable.FLOAT8, field);
			}
			else if (javaType.compareTo("java.lang.String") == 0)
			{
				addOneFieldToRecord(record, GPDBWritable.VARCHAR, field.get(userObject));
			}
			else if (javaType.compareTo("[Ljava.lang.String;") == 0)
			{
				ret = SetArrayField(record, GPDBWritable.VARCHAR, field);
			}
			else if (javaType.compareTo("float") == 0)
			{
				addOneFieldToRecord(record, GPDBWritable.REAL, field.get(userObject));
			}			
			else if (javaType.compareTo("[F") == 0)
			{
				ret = SetArrayField(record, GPDBWritable.REAL, field);
			}
			else if (javaType.compareTo("long") == 0)
			{
				addOneFieldToRecord(record, GPDBWritable.BIGINT, field.get(userObject));
			}			
			else if (javaType.compareTo("[J") == 0)
			{
				ret = SetArrayField(record, GPDBWritable.BIGINT, field);
			}
			else if (javaType.compareTo("[B") == 0)
			{
				addOneFieldToRecord(record, GPDBWritable.BYTEA, field.get(userObject));
			}
			else 
			{
				throw new UnsupportedTypeException("Unsupported java type " + javaType + 
						                           " (simple name: " + field.getType().getSimpleName() + ")");
			}
		}
		catch (IllegalAccessException ex)
		{
			throw new BadRecordException(ex);
		}
		return ret;
	}
	
	/*
	 * Reads data from inputStream and set customWritable fields. 
	 */
	public OneRow setFields(DataInputStream inputStream) throws Exception 
	{
		int recordkeyIndex = (inputData.getRecordkeyColumn() == null) ? RECORDKEY_UNDEFINED :
			inputData.getRecordkeyColumn().columnIndex();
		Writable key = null;
		
		gpdbWritable.readFields(inputStream);
		/*
		 * if the stream is empty (EOF), readFields() 
		 * will return without filling the fields.
		 */
		if (gpdbWritable.isEmpty())
		{
			Log.debug("Reached end of stream");
			return null;
		}
		
		gpdbWritableMapper = new GPDBWritableMapper(gpdbWritable);
		
		// convert to custom writable
		// go over fields in gpdbWritable one by the other, extract value and set it in userObject.
		int colIdx = 0;
		for (Field field : fields)
		{
			/*
			 * extract recordkey based on the column descriptor type
			 * and add to OneRow.key
			 */
			if (colIdx == recordkeyIndex) {
				key = populateKey(colIdx);
				++colIdx;
			}
			
			if (Modifier.isPrivate(field.getModifiers())) 
				continue;
			
			colIdx = populateField(field, colIdx);
		}
		
		return new OneRow(key, userObject);
	}
	
	/*
	 * Populate field with data from gpdbWritable.
	 * Field can be an array - in this case a few fields will be extracted from gpdbWritable.
	 * colIdx - the index of relevant field in gpdbWritable.
	 * Returned index is the next field in gpdbWritable (in case of array it will be colIdx + array length).
	 */
	int populateField(Field field, int colIdx) throws BadRecordException
	{		
		String javaType = field.getType().getName();
		boolean isArray = GPDBWritableMapper.isArray(javaType);
		int curColIdx = colIdx;
		Object value = null;
		
		try
		{
			gpdbWritableMapper.setDataType(javaType);
			if (isArray)
			{
					value = field.get(userObject);
					int length = Array.getLength(value);

					for (int j = 0; j < length; j++, curColIdx++)
					{
						Object obj = gpdbWritableMapper.getData(curColIdx);
						Array.set(value, j, obj);
					}
			}
			else // not array
			{ 
				value = gpdbWritableMapper.getData(curColIdx);		
				++curColIdx;
			}
	
			field.set(userObject, value);
		}
		catch (IllegalAccessException ex)
		{
			throw new BadRecordException(ex);
		} 
		catch (TypeMismatchException ex) 
		{
			throw new BadRecordException(ex);
		} 
		catch (UnsupportedTypeException ex) 
		{
			throw new BadRecordException(ex);
		}
    
		return curColIdx;
	}

	/**
	 * Reads data in column index colIdx of current row,
	 * and converts to a Writable object matching the 
	 * HAWQ field definition.
	 * If the colIdx is illegal (out of bounds), 
	 * or the data conversion fails (value doesn't match given type),
	 * a BadRecordException is thrown. 
	 *  
	 * @param colIdx column index to read
	 * @return Writable object containing the key value
	 * @throws BadRecordException in case of wrong colIdx or data not matching HAWQ type.
	 */
	Writable populateKey(int colIdx) throws BadRecordException {
		
		Writable key = null;
					
		gpdbWritableMapper.setDataType(inputData.getRecordkeyColumn().columnTypeCode());
		try {
			key = recordkeyAdapter.convertKeyValue(gpdbWritableMapper.getData(colIdx));
		} catch (TypeMismatchException ex) {
			throw new BadRecordException(ex);
		} catch (ClassCastException ex) {
			throw new BadRecordException(ex);
		}
		
		return key;
	}
	
	
	void InitInputObject() throws Exception
	{
		Class userClass = Class.forName(inputData.srlzSchemaName());

		//Use reflection to list methods and invoke them
		methods = userClass.getMethods();
		fields = userClass.getDeclaredFields();
		userObject = userClass.newInstance();

		// find the index of method readFields
		for (int i = 0; i < methods.length; i++) 
		{
			if (methods[i].getName().compareTo("readFields") == 0) 
			{
				index_of_readFields = i;
				break;
			}
		}
		
		// fields details:
		if (Log.isDebugEnabled())
		{
			for (int i = 0; i < fields.length; i++)
			{
				Field field = fields[i];
				String javaType = field.getType().getName();
				boolean isPrivate = Modifier.isPrivate(field.getModifiers());
				
				Log.debug("Field #" + i + ", name: " + field.getName() + 
						" type: " + javaType + ", " + 
						(GPDBWritableMapper.isArray(javaType) ? "Array" : "Primitive") + ", " +
						(isPrivate ? "Private" : "accessible") + " field");

			}
		}
		
	} 
}
