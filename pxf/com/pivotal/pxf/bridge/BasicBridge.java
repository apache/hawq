
package com.pivotal.pxf.bridge;

import java.nio.charset.CharacterCodingException;
import java.io.CharConversionException;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.zip.ZipException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.pivotal.pxf.accessors.Accessor;
import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.format.BridgeOutputBuilder;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.resolvers.Resolver;
import com.pivotal.pxf.utilities.HDMetaData;

/*
 * class BasicBridge is an abstract class implementing the "Template Method" design pattern
 * BasicBridge implements the IBridge interface. In order to implement the IBridge methods,
 * BasicBridge uses the methods of two other "internal" abstract classes Accessor and Resolver.
 * The use of these two internal abstract classes (Accessor and Resolver) inside BasicBridge 
 * represents in fact the skeleton of the data access algorithm of the Bridge framework.
 * The actual details of file access and records deserialization for given file types or serialization methods
 * are located inside the implementations of the abstract classes Accessor and Resolver.
 * The user cannot instantiate BasicBridge since it is abstract. 
 * Instead the user will instantiate a specialization of BasicBridge (like for instance GpHdfsBridge). 
 * The sole purpose of the BasicBridge specialization class is to instantiate the actual implementations 
 * of Accessor and Resolver, and pass them to the BasicBridge class who uses the 
 * interfaces without being aware of the actual implementations which hold the details of file access 
 * and deserialization.
 */
abstract class BasicBridge implements IBridge
{
    HDMetaData conf = null;
	Accessor fileAccessor = null;
	Resolver fieldsResolver = null;
	BridgeOutputBuilder outputBuilder = null;
    
    private Log Log;
    
	/*
	 * C'tor - set the implementation of the bridge
	 */
	BasicBridge(HDMetaData configuration, Accessor accessor, Resolver resolver)
	{
        Log = LogFactory.getLog(BasicBridge.class);
        conf = configuration;
		fileAccessor = accessor;
		fieldsResolver = resolver;
		outputBuilder = new BridgeOutputBuilder(conf);
	}

	/*
	 * Accesses the underlying HDFS file
	 */
	public boolean BeginIteration() throws Exception
	{
		return fileAccessor.Open();
	}

	/* 
	 * Fetch next object from file and turn it into a record that the GPDB backend can process
	 */
	public Writable GetNext() throws Exception
	{
		Writable output = null;
		OneRow onerow = null;
		try
		{
			onerow = fileAccessor.LoadNextObject();
			if (onerow == null)
			{
				fileAccessor.Close();
				return null;
			}

			output = outputBuilder.makeOutput(fieldsResolver.GetFields(onerow));
		}
		catch (IOException ex)
		{
			if (!isDataException(ex))
				throw ex;
			output = outputBuilder.getErrorOutput(ex);
		}
		catch (BadRecordException ex)
		{
            String row_info = "null";
            if (onerow != null)
            {
                row_info = onerow.toString();
            }
            if (ex.getCause() != null)
            {
                Log.debug("BadRecordException " + ex.getCause().toString() + ": " + row_info);
            }
            else
            {
                Log.debug(ex.toString() + ": " + row_info);
            }
			output = outputBuilder.getErrorOutput(ex);
		}
		
		return output;
	}
		
	/*
	 * There are many exceptions that inherit IOException. Some of them like EOFException are generated
	 * due to a data problem, and not because of an IO/connection problem as the father IOException
	 * might lead us to believe. For example, an EOFException will be thrown while fetching a record
	 * from a sequence file, if there is a formatting problem in the record. Fetching record from
	 * the sequence-file is the responsibility of the accessor so the exception will be thrown from the
	 * accessor. We identify this cases by analyzing the exception type, and when we discover that the
	 * actual problem was a data problem, we return the errorOutput GPDBWritable.
	 */
	private boolean isDataException(IOException ex)
	{
		if (ex instanceof EOFException || ex instanceof CharacterCodingException ||
			ex instanceof CharConversionException || ex instanceof UTFDataFormatException || 
			ex instanceof ZipException)
				return true;
		
		return false;
	}	
}
