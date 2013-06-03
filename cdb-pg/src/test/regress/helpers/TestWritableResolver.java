import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.Resolver;
import com.pivotal.pxf.utilities.HDFSMetaData;
import com.pivotal.pxf.utilities.RecordkeyAdapter;


/*
 * Class WritableResolver handles deserialization of records that were serialized 
 * using Hadoop's Writable serialization framework. WritableResolver implements
 * IFieldsResolver exposing one method: GetFields
 */
public class TestWritableResolver extends  Resolver
{
    private HDFSMetaData connectorConfiguration;
	private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();

	// reflection fields
	private Object userObject = null;
	private Method[] methods = null;
	private Field[] fields = null;
	private int index_of_readFields = 0;

	public TestWritableResolver(HDFSMetaData conf) throws Exception
	{
		super(conf);
		/* 
		 * The connectorConfiguration variable will be discarded once we remove all specialized MetaData classes and remain 
		 * only with BaseMetaData which holds the sequence of properties
		 */
		connectorConfiguration = (HDFSMetaData)this.getMetaData();	
		
		InitInputObject();
	}

	/*
	 * GetFields returns a list of the fields of one record.
	 * Each record field is represented by a OneField item.
	 * OneField item contains two fields: an integer representing the field type and a Java
	 * Object representing the field value.
	 */
	public List<OneField> GetFields(OneRow onerow) throws Exception
	{
		userObject = onerow.getData();
		List<OneField> record =  new LinkedList<OneField>();
		int recordkeyIndex = (connectorConfiguration.getRecordkeyColumn() == null) ? -1 :
			connectorConfiguration.getRecordkeyColumn().columnIndex();
		int currentIdx = 0;

		for (Field field : fields)
		{
			if (currentIdx == recordkeyIndex)
				currentIdx += recordkeyAdapter.appendRecordkeyField(record, connectorConfiguration, onerow);
				
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
			if (javaType.compareTo("int") == 0)
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
		}
		catch (IllegalAccessException ex)
		{
			throw new BadRecordException();
		}
		return ret;
	}

	void InitInputObject() throws Exception
	{
		Class userClass = Class.forName(connectorConfiguration.srlzSchemaName());

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
	} 
}
