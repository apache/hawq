package com.pivotal.pxf.resolvers;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.hadoop.io.GPDBWritable;


/*
 * PbResolver class in not usable. It should  NOT be instantiated!
 * It's code needs to be refactored, so that the PB serialized
 * records could be read from a sequence file. 
 * Still, most of the code will retain it's current form, so I chose to leave the 
 * class in Perforce. 
 * But, the class is no longer instantiated in the GPHdfsBridge
 */
public class PbResolver implements IFieldsResolver
{
	private String protoFile;
	private String containerMsg;
	// the reflection instances
	private DynamicMessage m = null;
	private Descriptors.FieldDescriptor key = null;
	private int num_records = 0;
	private int cur_record = 0;
	private Object curObject = null;
	private boolean firstTime = true;
	
	public PbResolver(String parProtoFile, String parContainerMsg) throws Exception
	{
		throw new Exception("Protocol Buffer resolver not yet supported");
		/*
		protoFile = parProtoFile;
		containerMsg = parContainerMsg;
		*/
	}
	
	/*
	 LoadNextObject interface method was removed from IFieldsResolver and added to 
	 IHdfsFileAccessor, but the logic inside the function is still required by PB and should
	 be integrated in the next version, when PB will be rendered usable again, to be read from
	 a sequence file 
	*/
	/*
	public boolean LoadNextObject(InputStream in) throws Exception
	{
		// 1. The PB DynamicMessage that encapsulates all records is loaded in one call from  the data file
		//    we do this in the open() method which is called just once.
		//    the other times LoadNextObject we just bring the next encapsulated message inside the DynamicMessage
		if ( firstTime )
		{
			boolean success = open(in);
			if ( !success )
				return false;
			firstTime = false;
		}
		
		// 2. If we reached the end of the records sequence, we leave
		if (cur_record == num_records)
			return false;
		
		// 3. Bring the next encapsulated message inside the DynamicMessage
		curObject = m.getRepeatedField(key, cur_record);
		cur_record++;
				
		return true;
	}
	*/
		
	public List<OneField> GetFields(OneRow onerow)
	{
		java.util.List<OneField> list = new java.util.LinkedList<OneField>();
		
		java.util.Iterator it = getFieldsIterator(onerow.getData());
		while(it.hasNext())
		{
			
			java.util.Map.Entry en =(java.util.Map.Entry)it.next();
			Descriptors.FieldDescriptor key=(Descriptors.FieldDescriptor)en.getKey();
			Object val = en.getValue();
			
			if ( key.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE) )
				// our original record is represented by a PB message (it is the input parameter obj)
				// It turns out that one of the fields of the record-message, is a message on its own - an embedded message
			{
				// we can deal with a simple embedded message
				if ( key.isRepeated() == false )
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
	
	boolean open(InputStream in_stream)
	{
		// PB deserialization using DynamicMessage
		
		try 
		{
			// 1. creating a FileDescriptorSet by loading the schema *.desc file
			FileInputStream proto_in = new FileInputStream(protoFile);
			DescriptorProtos.FileDescriptorSet file_descriptor_set = DescriptorProtos.FileDescriptorSet.parseFrom(proto_in);
			
			// 2. FileDescriptorSet can theoretically contain many *proto files each one incapsulated inside
			//    a FileDescriptorProto. We get the first one - for all our cases there is only one *proto file
			DescriptorProtos.FileDescriptorProto  file_descriptor_proto = file_descriptor_set.getFile(0);
			
			// 3. Building a FileDescriptor based on the FileDescriptorProto
			Descriptors.FileDescriptor fileDescr = 
				Descriptors.FileDescriptor.buildFrom(file_descriptor_proto, new Descriptors.FileDescriptor[0]);
			
			// 4. The FileDescriptor contains a sequence of Descriptor instances  - one for each message defined in the *.proto file
			//    We get the Descriptor that represents the container message. The container message has only one repeated field - the 
			//    actual record message
			Descriptors.Descriptor descriptor = fileDescr.findMessageTypeByName(containerMsg);
			
			// 5. Creating a DynamicMessage while LOADING ALL the data using the  Descriptor that represents the container message
			m = DynamicMessage.parseFrom(descriptor, in_stream);			
		}
		catch (Exception e)
		{
			return false;
		}
		
		// 6. The DynamicMessage m, representing the container message, has only one repeated field (each repetion is one record in the data file)
		//    The repeated field is represented by a list of DynamicMessages - this list is the value of tone map pair.
		//    For our case there is only one pair. We are interested in the key of the pair which we will use in the
		//    implementation of loadNextObject to get at each record in the list of records.		
		boolean ret = AccessRecordsList();
		
		return ret;
	}
	
	boolean AccessRecordsList()
	{
		Map<Descriptors.FieldDescriptor,Object> map = m.getAllFields();
		Set s = map.entrySet();
		Iterator it = s.iterator();
		if ( !it.hasNext() )
			return false;
		
		// key=value separator this by Map.Entry to get key and value
		java.util.Map.Entry en =(java.util.Map.Entry)it.next();
		
		// getKey is used to get key of Map
		key=(Descriptors.FieldDescriptor)en.getKey();
		if (key == null)
			return false;
		
		num_records = m.getRepeatedFieldCount(key);
		if (num_records == 0)
			return false;
		
		cur_record = 0;
		
		return true;
	}
	
	java.util.Iterator getFieldsIterator(Object obj)
	{
		DynamicMessage record = (DynamicMessage)obj;
		java.util.Map<Descriptors.FieldDescriptor,Object> map = record.getAllFields();
		java.util.Set s = map.entrySet();
		java.util.Iterator it = s.iterator();
		
		return it;
	}
	
	OneField makeOneField(Descriptors.FieldDescriptor key, Object val)
	{
		OneField oneField = new OneField();
		
		oneField.type = fromPBtoGP(key.getJavaType());
		oneField.val = val;
		
		if ( key.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.ENUM) )
		{
			Descriptors.EnumValueDescriptor eno = (Descriptors.EnumValueDescriptor)val;
			int num = eno.getNumber();
			oneField.val = num;
		}
		
		return oneField;
	}	
	
	int fromPBtoGP( Descriptors.FieldDescriptor.JavaType javaType)
	{
		int gpType = 0;
		
		if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.INT) )
		{
			gpType = GPDBWritable.INTEGER;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.LONG) )
		{
			gpType = GPDBWritable.BIGINT;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM) )
		{
			gpType = GPDBWritable.INTEGER;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN) )
		{
			//gpType = ;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE) )
		{
			gpType = GPDBWritable.FLOAT8;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT) )
		{
			gpType = GPDBWritable.REAL;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.STRING) )
		{
			gpType = GPDBWritable.VARCHAR;
		}
		else if ( javaType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING) )
		{
			gpType = GPDBWritable.BYTEA;
		}
		
		return gpType;
	}
}	

