package com.pivotal.pxf.accessors;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;

/*
 * Specialization of SplittableFileAccessor for AVRO files
 */
public class AvroFileAccessor extends HdfsSplittableDataAccessor
{
	private Schema schema = null;
	private AvroWrapper<GenericRecord>  avroWrapper = null;

	/*
	 * C'tor
	 * Creates the job configuration object JobConf, and accesses the avro file to fetch
	 * the avro schema
	 */
	public AvroFileAccessor(InputData input) throws Exception
	{
		// 1. Call the base class
		super(input,
			  new AvroInputFormat<GenericRecord>());
		
		// 2. Acessesing the avro file through the "unsplittable" API just to get the schema.
		//    The splittable API (AvroInputFormat) which is the one we will be using to fetch 
		//    the records, does not suport getting the avro schema yet.
		Path p = new Path(inputData.path());
		FsInput inStream = new FsInput(p, conf);
		DatumReader<GenericRecord> dummyReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dummyFileReader = new DataFileReader<GenericRecord>(inStream, dummyReader);
		schema = dummyFileReader.getSchema();
		
		// 3. Pass the schema to the inputData for the AvroResolver to fetch
		inputData.SetAvroFileSchema(schema);
		
		// 4. Pass the schema to the AvroInputFormat
		AvroJob.setInputSchema(jobConf, schema);
		
		// 5. The avroWrapper required for the iteration
		avroWrapper = new AvroWrapper<GenericRecord>();
	}
	
	/*
	 * Override virtual method to create specialized record reader
	 */
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return new AvroRecordReader(jobConf, (FileSplit)split);
	}		

	/*
	 * LoadNextObject
	 * The AVRO accessor is currently the only specialized accessor that overrides this method
	 * This happens, because the special AvroRecordReader.next() semantics (use of the AvroWrapper), so it
	 * cannot use the RecordReader's default implementation in SplittableFileAccessor
	 */	
	public OneRow LoadNextObject() throws IOException
	{
		if (reader.next(avroWrapper, NullWritable.get())) // if there is one more record in the current split
		{
			return new OneRow(null, avroWrapper.datum());
		}
		else if (getNextSplit()) // the current split is exhausted. try to move to the next split.
		{
			if (reader.next(avroWrapper, NullWritable.get()))
				return new OneRow(null, avroWrapper.datum());
			else 
				return null;
		}

		// if neither condition was met, it means we already read all the records in all the splits, and
		// in this call record variable was not set, so we return null and thus we are signaling end of 
		// records sequence - in this case avroWrapper.datum() will be null
		return null;
	}
}
