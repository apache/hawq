import java.util.List;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.pivotal.pxf.api.format.OneField;
import com.pivotal.pxf.api.format.OneRow;
import static com.pivotal.pxf.api.io.DataType.*;

import com.pivotal.pxf.api.resolvers.ReadResolver;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

/*
 * Implementation for protocol-buffers of Resolver
 */
public class ProtobufResolver extends Plugin implements ReadResolver
{
	// the reflection instances
	private DynamicMessage m = null;
	private Descriptors.FieldDescriptor key = null;
	private Object curObject = null;
	
	/*
	 * C'tor
	 */
	public ProtobufResolver(InputData input)
	{
		super(input);
	}

	/*
	 * Interface method - returns a list of OneField objects - each object
	 * represents a field in a record
	 */	
	public List<OneField> getFields(OneRow onerow)
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
				if (!key.isRepeated())
				{
					java.util.List<OneField> embList = getFields(new OneRow(null, val));
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
						java.util.List<OneField> embRepList = getFields(new OneRow(null, one_of_many));
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
    int fromPBtoGP(Descriptors.FieldDescriptor.JavaType javaType) {
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.INT)) {
            return INTEGER.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.LONG)) {
            return BIGINT.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
            return INTEGER.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN)) {
            return BOOLEAN.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE)) {
            return FLOAT8.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
            return REAL.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.STRING)) {
            return VARCHAR.getOID();
        }
        if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING)) {
            return BYTEA.getOID();
        }

        return 0;
    }
}
