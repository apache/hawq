import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import com.pivotal.pxf.accessors.HdfsAtomicDataAccessor;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;

/*
 * Specialization of HdfsAtomicDataAccessor for protocol-buffer files
 */
public class ProtobufFileAccessor extends HdfsAtomicDataAccessor
{
	private int num_records = 0;
	private int cur_record = 0;
	private String protoFile = null;
	private String containerMsg = null;
	// the reflection instances
	private DynamicMessage m = null;
	private Descriptors.FieldDescriptor key = null;
	private Object curObject = null;	
	/*
	 * C'tor
	 */ 	
	public ProtobufFileAccessor(InputData inputData) throws Exception
	{
		super(inputData);		
		protoFile = inputData.srlzSchemaName();
		
		// containerMessage is the name of a Message class in protocol-buffer that acts as 
		// a container to the records - where all records are of the same message type (different
		// type than the containerMessage).
		// Normally, this value should be received as a parameter from the the InputData.
		// We supply it hardcoded as a temporary solution, until there will be a mechanism  for 
		// transferring additional parameters between the backend and the HDFSReader
		containerMsg = "People";
	}
	
	public boolean Open() throws Exception
	{		
		if (!super.Open())
			return false;
		return open(inp);
	}

	/*
	 * LoadNextObject
	 * Fetches one record from the  file. The record is returned as a Java object.
	 */			
	public OneRow LoadNextObject() throws IOException
	{
		// 1. The base class makes sure we can proceed - it tests that we are running in segment 0
		if (super.LoadNextObject() == null)
			return null;

		// 2. If we reached the end of the records sequence, we leave
		if (cur_record == num_records)
			return null;
		
		// 3. Bring the next encapsulated message inside the DynamicMessage
		curObject = m.getRepeatedField(key, cur_record);
		cur_record++;
		
		return new OneRow(null, curObject);
	}
	
	/*
	 * Opens the protocols-buffer file
	 */
	private boolean open(InputStream in_stream) throws IOException, DescriptorValidationException
	{
		// 1. creating a FileDescriptorSet by loading the schema *.desc file
		DescriptorProtos.FileDescriptorSet file_descriptor_set = DescriptorProtos.FileDescriptorSet.parseFrom(openExternalSchema());
		
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
		
		// 6. The DynamicMessage m, representing the container message, has only one repeated field (each repetion is one record in the data file)
		//    The repeated field is represented by a list of DynamicMessages - this list is the value of tone map pair.
		//    For our case there is only one pair. We are interested in the key of the pair which we will use in the
		//    implementation of loadNextObject to get at each record in the list of records.		
		return AccessRecordsList();
	}
	
	/*
	 * Get file iteration handle and boundaries: the initial key value and number of records inside
	 */
	boolean AccessRecordsList()
	{
		Map<Descriptors.FieldDescriptor,Object> map = m.getAllFields();
		Set s = map.entrySet();
		Iterator it = s.iterator();
		if (!it.hasNext())
			return false;
		
		// key = value separator this by Map.Entry to get key and value
		java.util.Map.Entry en = (java.util.Map.Entry)it.next();
		
		// getKey is used to get key of Map
		key = (Descriptors.FieldDescriptor)en.getKey();
		if (key == null)
			return false;
		
		num_records = m.getRepeatedFieldCount(key);
		if (num_records == 0)
			return false;
		
		cur_record = 0;
		return true;
	}	

	InputStream openExternalSchema()
	{
		ClassLoader loader = this.getClass().getClassLoader();
		InputStream result = loader.getResourceAsStream(protoFile);

		if (result == null)
			throw new IllegalArgumentException("Can't find schema " + protoFile + " in classpath");

		return result;
	}
}
