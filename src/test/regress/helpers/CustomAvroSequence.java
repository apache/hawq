import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;
import java.net.URISyntaxException;

import java.io.ByteArrayOutputStream;
import java.util.Calendar;
import java.sql.Timestamp;

/* 
 * Parameters:
 * <outdir> <outfile> <0/1> <schema>
 * outdir - directory for output
 * outfile - name of output file
 * 0 - read
 * 1 - write
 * schema - path for schema
 */
public class CustomAvroSequence
{
	private Configuration conf;

	private String        outPath;
	private String        outFile;

	private String        fullPath;
	static private int           do_write; // 1 - Write, 0 - read
	static private String schema = "CustomAvro.avsc";


	public CustomAvroSequence(String[] args)
	{
		conf        = new Configuration();
		conf.set("fs.defaultFS", "file://./");

		outPath     = args[0];
		outFile     = args[1];
		do_write    = Integer.parseInt(args[2]);
		schema		= args[3];

		URI outputURI = null;
		try
		{
			outputURI = new URI(outPath);
		} catch (URISyntaxException e)
		{
		}

		fullPath = outputURI.getPath() + Path.SEPARATOR + outFile;
		System.out.println(fullPath);
	}

	void WriteFile() throws Exception
	{
		System.out.println("WriteFile");

		FileSystem fs = null;

		fs = FileSystem.get(URI.create(fullPath), conf);

		IntWritable key = new IntWritable();
		BytesWritable val = new BytesWritable();
		Path path = new Path(outPath + Path.SEPARATOR + outFile);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), val.getClass());

		for (int i=20; i<50; i++)
		{
			int num1 = i;

			Timestamp tms = new Timestamp(Calendar.getInstance().getTime().getTime());
			CustomAvroRecInSequence cust = new CustomAvroRecInSequence(schema, tms, num1, 10*num1, 20*num1);

			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			cust.serialize(stream);
			val = new BytesWritable(stream.toByteArray());
			writer.append(key, val);
		}

		IOUtils.closeStream(writer);

	}

	void ReadFile()  throws Exception
	{
		System.out.println("ReadFile\n");

		FileSystem fs = null;
		FSDataInputStream inp = null;

		fs = FileSystem.get(URI.create(fullPath), conf);
		Path p = new Path(outPath + Path.SEPARATOR + outFile);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
		IntWritable key = (IntWritable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		BytesWritable val = (BytesWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);

		System.out.println("The class field types are:");
		System.out.println("\n\n");

		CustomAvroRecInSequence cust = new CustomAvroRecInSequence(schema);
		while(reader.next(key,val))
		{
			cust.deserialize(val.getBytes());

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

			// 7. boolean
			boolean bl = cust.GetBool();

			System.out.println("bl: " + Boolean.toString(bl));

			System.out.println("#############################################################################################");
		}
	}

	public static void main(String[] args) throws Exception
	{

		CustomAvroSequence mgr = new CustomAvroSequence(args);
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
