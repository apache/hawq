package com.pivotal.pxf.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.utilities.InputData;

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
	
	GPWritableFileCreator(String outFullPath, int parKey, InputData input)
	{
		path = new Path(outFullPath);
		key = new LongWritable(parKey);
		try 
		{
			Class recordClass = (input.outputFormat() == OutputFormat.FORMAT_TEXT) ? 
						SimpleText.class : GPDBWritable.class;
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
