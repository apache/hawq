import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.IFieldsResolver;
import com.pivotal.pxf.utilities.HDFSMetaData;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;

/*
 * Implementation for protocol-buffers of IFieldsResolver
 */
public class ProtobufResolver implements  IFieldsResolver
{
	private HDFSMetaData metaData;
	// the reflection instances
	private DynamicMessage m = null;
	private Descriptors.FieldDescriptor key = null;
	private Object curObject = null;
	
	/*
	 * C'tor
	 */
	public ProtobufResolver(HDFSMetaData meta)
	{
		metaData = meta;
	}

	/*
	 * Interface method - returns a list of OneField objects - each object
	 * represents a field in a record
	 */	
	public List<OneField> GetFields(OneRow onerow)
	{
		java.util.List<OneField> list = new java.util.LinkedList<OneField>();
		
		java.util.Iterator it = getFieldsIterator(onerow.getData());
		while(it.hasNext())
		{
			
			java.util.Map.Entry en = (java.util.Map.Entry)it.next();
			Descriptors.FieldDescriptor key = (Descriptors.FieldDescriptor)en.getKey();
			Object val = en.getValue();
			
			if (key.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE))
				// our original record is represented by a PB message (it is the input parameter obj)
				// It turns out that one of the fields of the record-message, is a message on its own - an embedded message
			{
				// we can deal with a simple embedded message
				if (key.isRepeated() == false)
				{
					java.util.List<OneField> embList = GetFields(new OneRow(null, val));
					list.addAll(embList);
				}
				// or a repeated embedded message (that is a list of embedded messages)
				else 
				{ 	
					DynamicMessage record = (DynamicMessage)onerow.getData();
					int num_records = record.getRepeatedFieldCount(key);
					for (int i = 0; i < num_records; i++)
					{
						Object one_of_many = record.getRepeatedField(key, i);
						java.util.List<OneField> embRepList = GetFields(new OneRow(null, one_of_many));
						list.addAll(embRepList);
					}
				}
			}
			else
			{
				OneField oneField = makeOneField(key, val);
				list.add(oneField);
			}
		}						
		
		return list;
	}

	/*
	 * Access the PB iterator
	 */	
	java.util.Iterator getFieldsIterator(Object obj)
	{
		DynamicMessage record = (DynamicMessage)obj;
		java.util.Map<Descriptors.FieldDescriptor,Object> map = record.getAllFields();
		java.util.Set s = map.entrySet();
		java.util.Iterator it = s.iterator();
		
		return it;
	}

	/*
	 * Helper for setting a field
	 */	
	OneField makeOneField(Descriptors.FieldDescriptor key, Object val)
	{
		OneField oneField = new OneField();
		
		oneField.type = fromPBtoGP(key.getJavaType());
		oneField.val = val;
		
		if (key.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.ENUM))
		{
			Descriptors.EnumValueDescriptor eno = (Descriptors.EnumValueDescriptor)val;
			int num = eno.getNumber();
			oneField.val = num;
		}
		
		return oneField;
	}	

	/*
	 * Translating java types to GPDBWritable types
	 */	
	int fromPBtoGP(Descriptors.FieldDescriptor.JavaType javaType)
	{
		int gpType = 0;
		
		if (javaType.equals(Descriptors.FieldDescriptor.JavaType.INT))
		{
			gpType = GPDBWritable.INTEGER;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.LONG))
		{
			gpType = GPDBWritable.BIGINT;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM))
		{
			gpType = GPDBWritable.INTEGER;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN))
		{
			//gpType = ;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE))
		{
			gpType = GPDBWritable.FLOAT8;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT))
		{
			gpType = GPDBWritable.REAL;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.STRING))
		{
			gpType = GPDBWritable.VARCHAR;
		}
		else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING))
		{
			gpType = GPDBWritable.BYTEA;
		}
		
		return gpType;
	}
}