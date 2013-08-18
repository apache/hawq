import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;

import java.net.URI;
import java.net.URISyntaxException;
import java.lang.UnsupportedOperationException;


import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.sql.Timestamp;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.AvroJob;

import java.io.DataInput;

class AvroReporter implements Reporter
{
	private Counters counters;
	AvroReporter()
	{
		counters = new Counters();
	}

	public void progress()
	{
	}

	public float getProgress()
	{
		throw new RuntimeException("not implemented");
	}

	public void write(DataOutput out) throws IOException
	{
	}

	public void readFields(DataInput in) throws IOException
	{
	}

	public void setStatus (String status) {}

	public Counters.Counter getCounter(Enum<?> name)
	{
		return counters.findCounter(name);
	}

	public Counters.Counter getCounter(String group, String name)
	{
		return counters.findCounter(group, name);
	}

	public void incrCounter(Enum<?> key, long amont) {}

	public void incrCounter(String group, String counter,long amont) {}

	public InputSplit getInputSplit() throws  UnsupportedOperationException {
		return new InputSplit ()
		{
			public void write(DataOutput out) throws IOException {}
			public void readFields(DataInput in) throws IOException {}
			public long getLength() { return 0; }
			public String[] getLocations() { return new String [2]; }
		};
	}
}

/* 
 * Parameters:
 * <outdir> <outfile> <0/1> <schema>
 * outdir - directory for output
 * outfile - name of output file
 * 0 - read
 * 1 - write
 * schema - path for schema
 */
public class CustomAvroFile
{
	private Configuration conf;

	private String        outPath;
	private String        outFile;

	private String        fullPath;
	static private int           do_write; // 1 - Write, 0 - read
	static private String schemaName = "CustomAvro.avsc";
	AvroReporter reporter;

	public CustomAvroFile(String[] args)
	{
		conf        = new Configuration();
		conf.set("fs.defaultFS", "file://./");

		outPath     = args[0];
		outFile     = args[1];
		do_write    = Integer.parseInt(args[2]);
		schemaName	= args[3];

		URI outputURI = null;
		try
		{
			outputURI = new URI(outPath);
		} catch (URISyntaxException e)
		{
		}

		fullPath = outputURI.getPath() + Path.SEPARATOR + outFile;
		System.out.println(fullPath);

		reporter = new AvroReporter();
	}

	void WriteFile() throws Exception
	{
		System.out.println("WriteFile");

		FileSystem fs = FileSystem.get(URI.create(fullPath), conf);
		Path path = new Path(outPath + Path.SEPARATOR + outFile);
		OutputStream outStream = fs.create(path);

		Schema schema = Schema.parse(new FileInputStream(schemaName));
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema, outStream);

		for (int i=20; i<50; i++)
		{
			int num1 = i;

			Timestamp tms = new Timestamp(Calendar.getInstance().getTime().getTime());
			CustomAvroRecInFile cust = new CustomAvroRecInFile(schemaName, tms, num1, 10*num1, 20*num1, (i % 2) == 0);
			GenericRecord datum = cust.serialize();
			dataFileWriter.append(datum);
		}

		dataFileWriter.close();
	}

	void ReadFile()  throws Exception
	{
		System.out.println("ReadFile\n");

		/*
		Path p = new Path(outPath + Path.SEPARATOR + outFile);
		FsInput inStream = new FsInput(p, conf);
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(inStream, reader);
		GenericRecord record = null;
		*/

		String path = outPath + Path.SEPARATOR + outFile;
		System.out.println(path);

		// Get the schema
		Path p = new Path(path);
		FsInput inStream = new FsInput(p, conf);
		DatumReader<GenericRecord> dummyReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dummyFileReader = new DataFileReader<GenericRecord>(inStream, dummyReader);
		Schema schema = dummyFileReader.getSchema();


		JobConf jobConf = new JobConf(conf, CustomAvroFile.class);
		AvroInputFormat<GenericRecord> fformat = new org.apache.avro.mapred.AvroInputFormat<GenericRecord>();
		fformat.addInputPath(jobConf, new Path(path));
		Path[] dirs = fformat.getInputPaths(jobConf);
		System.out.println("dirs length: " + dirs.length);
		InputSplit[] splits = fformat.getSplits(jobConf, 4);
		System.out.println("splits length: " + splits.length);
		AvroJob.setInputSchema(jobConf, schema);
		//AvroRecordReader<GenericRecord> reader = new AvroRecordReader(jobConf, (FileSplit)splits[0]);
		AvroRecordReader<GenericRecord> reader =
		   (AvroRecordReader<GenericRecord>)fformat.getRecordReader(splits[0], jobConf, reporter);
		AvroWrapper<GenericRecord>  avroWrapper = new AvroWrapper<GenericRecord>();


		CustomAvroRecInFile cust = new CustomAvroRecInFile(schemaName);
		//while( dataFileReader.hasNext() )
		while ( reader.next(avroWrapper, NullWritable.get()) )
		{
			//record = dataFileReader.next(record);
			GenericRecord record = avroWrapper.datum();
			cust.deserialize(record);

			// 0. the timestamp
			String tms= cust.GetTimestamp();
			System.out.println(tms);

			// 1. the integers
			int [] num = cust.GetNum();
			for (int i = 0; i < num.length; i++)
			{
				Integer n = num[i];
				System.out.println(n.toString());
			}

			Integer int1 = cust.GetInt1();
			Integer int2 = cust.GetInt2();

			System.out.println(int1.toString());
			System.out.println(int2.toString());

			// 2. the strings
			String [] strings = cust.GetStrings();
			for (int i = 0; i < strings.length; i++)
			{
				System.out.println(strings[i]);
			}


			String st1 = cust.GetSt1();
			System.out.println(st1);

			// 3. the doubles
			double [] dubs = cust.GetDoubles();
			for (int i = 0; i < dubs.length; i++)
			{
				System.out.println(Double.toString(dubs[i]));
			}

			double db = cust.GetDb();
			System.out.println(Double.toString(db));

			// 4. the floats
			float [] fts = cust.GetFloats();
			for (int i = 0; i < fts.length; i++)
			{
				System.out.println(Float.toString(fts[i]));
			}

			float ft = cust.GetFt();
			System.out.println(Float.toString(ft));

			// 5. the longs
			long [] lngs = cust.GetLongs();
			for (int i = 0; i < lngs.length; i++)
			{
				System.out.println(Long.toString(lngs[i]));
			}

			long lng = cust.GetLong();
			System.out.println(Long.toString(lng));

			// 6. the bytes
			byte [] bts = cust.GetBytes();

			System.out.println("bts length:   " + bts.length);
			String sb = new String("");
			for (int i =0; i < bts.length; i++)
			{
				sb = sb + " " + bts[i];
			}
			System.out.println(sb);
			System.out.println(new String(bts));

			// 7. the boolean
			System.out.println(Boolean.toString(cust.GetBool()));

			System.out.println("#############################################################################################");
		}
	}

	public static void main(String[] args) throws Exception
	{

		CustomAvroFile mgr = new CustomAvroFile(args);
		if (do_write == 0)
		{
			mgr.ReadFile();
		}
		else
		{
			mgr.WriteFile();
		}
	}
}
