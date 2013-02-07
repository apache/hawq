package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/*
 * Class for creating a GPDBWritable sequence file based on the HDFS user input file.
 * We will use this class in order to simulate ET1 operation mode
 */
class GPWritableFileCreator
{
	Path path = null;
	Configuration conf = new Configuration();
	SequenceFile.Writer writer = null;
	LongWritable key = null;
	
	GPWritableFileCreator(String outFullPath, int parKey, HDMetaData meta)
	{
		path = new Path(outFullPath);
		key = new LongWritable(parKey);
		try 
		{
			Class recordClass = (meta.outputFormat() == OutputFormat.FORMAT_TEXT) ? 
						BridgeOutputBuilder.SimpleText.class : GPDBWritable.class;
			writer =  SequenceFile.createWriter(conf, 
												SequenceFile.Writer.file(path),
												SequenceFile.Writer.keyClass(LongWritable.class),
												SequenceFile.Writer.valueClass(recordClass));
		}
		catch (IOException ie)
		{
			System.out.println("GPWritableFileCreator c'tor received exception: " + ie.getMessage());
		}		
	}
	
	void writeOneRecord(Writable record)
	{		
		try 
		{
				writer.append(key, record);
		}
		catch (IOException ie)
		{
			System.out.println("GPWritableFileCreator.writeOneRecord received exception: " + ie.getMessage());
		}									
	}
	
	void finish()
	{
		try 
		{		
			writer.close();
		}
		catch (IOException ie)
		{
		}				
	}
}
