package com.pivotal.pxf.bridge;

import java.io.CharConversionException;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.charset.CharacterCodingException;
import java.util.zip.ZipException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.format.BridgeOutputBuilder;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.resolvers.IReadResolver;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Utilities;

/*
 * ReadBridge class creates appropriate accessor and resolver.
 * It will then create the correct output conversion
 * class (e.g. Text or GPDBWritable) and get records from accessor,
 * let resolver deserialize them and reserialize them using the
 * output conversion class.
 *
 * The class handles BadRecordException and other exception type
 * and marks the record as invalid for GPDB.
 */
public class ReadBridge implements IBridge
{
	IReadAccessor fileAccessor = null;
	IReadResolver fieldsResolver = null;
	BridgeOutputBuilder outputBuilder = null;

	private Log Log;

	/*
	 * C'tor - set the implementation of the bridge
	 */
	public ReadBridge(InputData input) throws Exception
	{
		fileAccessor = getFileAccessor(input);
		fieldsResolver = getFieldsResolver(input);
		outputBuilder = new BridgeOutputBuilder(input);
		Log = LogFactory.getLog(ReadBridge.class);
	}

	/*
	 * Accesses the underlying HDFS file
	 */
	public boolean beginIteration() throws Exception
	{
		return fileAccessor.openForRead();
	}

	/*
	 * Fetch next object from file and turn it into a record that the GPDB backend can process
	 */
	public Writable getNext() throws Exception
	{
		Writable output = null;
		OneRow onerow = null;
		try
		{
			onerow = fileAccessor.readNextObject();
			if (onerow == null)
			{
				fileAccessor.closeForRead();
				return null;
			}

			output = outputBuilder.makeOutput(fieldsResolver.getFields(onerow));
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

	public static IReadAccessor getFileAccessor(InputData inputData) throws Exception
	{
		return (IReadAccessor)Utilities.createAnyInstance(InputData.class, 
													 inputData.accessor(), 
													 "accessors", 
													 inputData);
	}

	public static IReadResolver getFieldsResolver(InputData inputData) throws Exception
	{
		return (IReadResolver)Utilities.createAnyInstance(InputData.class, 
													 inputData.resolver(), 
													 "resolvers", 
													 inputData);
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

	@Override
	public boolean setNext(DataInputStream inputStream) throws Exception 
	{
		throw new Exception("setNext is not implemented");
	}
}
