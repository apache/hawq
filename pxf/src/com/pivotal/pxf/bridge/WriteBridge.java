package com.pivotal.pxf.bridge;

import java.io.DataInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.pivotal.pxf.accessors.IWriteAccessor;
import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.resolvers.IWriteResolver;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;
import com.pivotal.pxf.utilities.Utilities;

/*
 * WriteBridge class creates appropriate accessor and resolver.
 * It reads data from inputStream by the resolver,
 * and writes it to the Hadoop storage with the accessor.
 */
public class WriteBridge implements IBridge
{
	IWriteAccessor fileAccessor = null;
	IWriteResolver fieldsResolver = null;

	private Log Log;

	/*
	 * C'tor - set the implementation of the bridge
	 */
	public WriteBridge(InputData input) throws Exception
	{
		fileAccessor = getFileAccessor(input);
		fieldsResolver = getFieldsResolver(input);
		Log = LogFactory.getLog(WriteBridge.class);
	}

	/*
	 * Accesses the underlying HDFS file
	 */
	public boolean beginIteration() throws Exception
	{
		return fileAccessor.openForWrite();
	}

	/*
	 * Read data from stream, convert it using WriteResolver into OneRow object, and
	 * pass to WriteAccessor to write into file.
	 */
	public boolean setNext(DataInputStream inputStream) throws Exception
	{

		OneRow	onerow = fieldsResolver.setFields(inputStream);

		if (onerow == null)
		{
			close();
			return false;
		}
		if (!fileAccessor.writeNextObject(onerow))
		{
			close();
			throw new BadRecordException();
		}

		return true;
	}
	
	private void close()  
	{
		try 
		{
			fileAccessor.closeForWrite();
		}
		catch (Exception e)
		{
			Log.warn("Failed to close bridge resources: " + e.getMessage());
		}
	}

	private static IWriteAccessor getFileAccessor(InputData inputData) throws Exception
	{
		return (IWriteAccessor)Utilities.createAnyInstance(InputData.class, 
				inputData.accessor(), 
				"accessors", 
				inputData);
	}

	private static IWriteResolver getFieldsResolver(InputData inputData) throws Exception
	{
		return (IWriteResolver)Utilities.createAnyInstance(InputData.class, 
				inputData.resolver(), 
				"resolvers", 
				inputData);
	}

	@Override
	public Writable getNext() throws Exception 
	{
		throw new Exception("getNext is not implemented");
	}
	
	@Override
	public boolean isThreadSafe() {
		return ((Plugin)fileAccessor).isThreadSafe() && ((Plugin)fieldsResolver).isThreadSafe();
	}
}
