import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Calendar;

/* 
 * CustomWritable class used to serialize and deserialize
 * data with the below public data types
 *
 * TODO: This should be dynamically compiled and used in data/pxf instead
 * of checked-in .class file
 */
public class CustomWritable implements Writable
{
	public String tms;
	public int [] num;
	public int int1;
	public int int2;
	public String [] strings;
	public String st1;
	
	private String pleaseIgnoreMe;
	
	public double [] dubs;
	public double db;
	public float [] fts;
	public float ft;
	public long [] lngs;
	public long lng;
	public byte [] bts;
	public boolean[] bools;
	public boolean bool;
	public short[] shrts;
	public short shrt;

	public CustomWritable()
	{
		// 0. Timestamp
		tms = new Timestamp(Calendar.getInstance().getTime().getTime()).toString();

		// 1. num array, int1, int2
		initNumArray();
		for (int i=0; i<num.length; i++)
			num[i] = 0;

		int1 = 0;
		int2 = 0;

		// 2. Init strings
		initStringsArray();
		for (int i=0; i<strings.length; i++)
			strings[i] = new String("");

		st1 = new String("");

		// 3. Init doubles
		initDoublesArray();
		for (int i=0; i<dubs.length; i++)
			dubs[i] = 0.0;
		db = 0.0;

		// 4. Init floats
		initFloatsArray();
		for (int i=0; i<fts.length; i++)
			fts[i] = 0.f;
		ft = 0.f;

		// 5. Init longs
		initLongsArray();
		for (int i=0; i < lngs.length; i++)
			lngs[i] = 0;
		lng = 0;

		// 6. Init bytes
		initBytesArray();
		bts = "Sarkozy".getBytes();

		// 7. Init booleans
		initBoolsArray();
		for (int i=0; i < bools.length; ++i)
			bools[i] = ((i % 2) == 0);
		bool = true;
		
		//8. Init shorts
		initShortsArray();
		for (int i=0; i < shrts.length; ++i)
			shrts[i] = 0;
		shrt = 0;
	}

	public CustomWritable(Timestamp tm, int i1, int i2, int i3)
	{
		// 0. Timestamp
		tms = tm.toString();

		// 1. num array, int1, int2
		initNumArray();		
		for (int k = 0; k < num.length; k++)
			num[k] = i1 * 10 * (k + 1); 

		int1   = i2;
		int2   = i3;

		// 2. Init strings
		initStringsArray();
		for (int k = 0; k < strings.length; k++)
			strings[k] = "strings_array_member_number___" + (k + 1);	

		st1 = new String("short_string___" + i1);

		// 3. Init doubles
		initDoublesArray();
		for (int k = 0; k < dubs.length; k++)
			dubs[k] = i1 * 10.0 * (k + 1);
		db = (i1 + 5) * 10.0;

		// 4. Init floats
		initFloatsArray();
		for (int k = 0; k < fts.length; k++)
			fts[k] = i1 * 10.f * 2.3f * (k + 1);
		ft = i1 * 10.f * 2.3f;

		// 5. Init longs
		initLongsArray();
		for (int i=0; i < lngs.length; i++)
			lngs[i] = i1 * 10 * (i + 3); 
		lng = i1 * 10 + 5; 

		// 6. Init bytes
		initBytesArray();
		bts = "Writable".getBytes();

		// 7. Init booleans
		initBoolsArray();
		for (int i=0; i < bools.length; ++i)
			bools[i] = ((i % 2) != 0);
		bool = false;
		
		// 8. Init shorts
		initShortsArray();
		for (int i=0; i < shrts.length; ++i)
			shrts[i] = (short)(i3 % 100);
		shrt = 100;
	}

	void initNumArray()
	{
		num = new int[2];		
	}

	void initStringsArray()
	{
		strings = new String[5];		
	}

	void initDoublesArray()
	{
		dubs = new double[2];		
	}

	void initFloatsArray()
	{
		fts = new float[2];		
	}

	void initLongsArray()
	{
		lngs = new long[2];		
	}

	void initBytesArray()
	{
		//bts = new byte[10];		
	}

	void initBoolsArray()
	{
		bools = new boolean[2];
	}
	
	void initShortsArray()
	{
		shrts = new short[4];
	}

	String GetTimestamp()
	{
		return tms;
	}

	int[] GetNum()
	{
		return num;
	}

	int GetInt1()
	{
		return int1;
	}

	int GetInt2()
	{
		return int2;
	}

	String [] GetStrings()
	{
		return strings;
	}

	String GetSt1()
	{
		return st1;
	}

	double [] GetDoubles()
	{
		return dubs;
	}

	double GetDb()
	{
		return db;
	}

	float [] GetFloats()
	{
		return fts;
	}

	float GetFt()
	{
		return ft;
	}

	long [] GetLongs()
	{
		return lngs;
	}

	long GetLong()
	{
		return lng;
	}

	byte [] GetBytes()
	{
		return bts;
	}

	boolean GetBool()
	{
		return bool;
	}

	boolean[] GetBools()
	{
		return bools;
	}
	
	short GetShort()
	{
		return shrt;
	}
	
	short[] GetShorts()
	{
		return shrts;
	}

	public void write(DataOutput out) throws IOException 
	{
		// 0. Timestamp
		Text tms_text = new Text(tms);
		tms_text.write(out);

		// 1. num, int1, int2
		IntWritable intw = new IntWritable();

		for (int i = 0; i < num.length; i++)
		{
			intw.set(num[i]);
			intw.write(out);
		}

		intw.set(int1);
		intw.write(out);

		intw.set(int2);
		intw.write(out);

		// 2. st1
		Text txt = new Text();

		for (int i = 0; i < strings.length; i++)
		{
			txt.set(strings[i]);
			txt.write(out);
		}		

		txt.set(st1);
		txt.write(out);

		// 3. doubles
		DoubleWritable dw = new DoubleWritable();
		for (int i = 0; i < dubs.length; i++)
		{
			dw.set(dubs[i]);
			dw.write(out);
		}

		dw.set(db);
		dw.write(out);

		// 4. floats
		FloatWritable fw = new FloatWritable();
		for (int i = 0; i < fts.length; i++)
		{
			fw.set(fts[i]);
			fw.write(out);
		}

		fw.set(ft);
		fw.write(out);

		// 5. longs
		LongWritable lw = new LongWritable();
		for (int i = 0; i < lngs.length; i++)
		{
			lw.set(lngs[i]);
			lw.write(out);
		}

		lw.set(lng);
		lw.write(out);

		// 6. bytes
		//BytesWritable btsw = new BytesWritable(bts);
		//btsw.write(out);
		BytesWritable btsw = new BytesWritable();
		btsw.setCapacity(bts.length);
		btsw.setSize(bts.length);
		btsw.set(bts,0, bts.length );
		btsw.write(out);		

		// 7. booleans
		BooleanWritable bw = new BooleanWritable();
		for (int i = 0; i < bools.length; ++i)
		{
			bw.set(bools[i]);
			bw.write(out);
		}
		bw.set(bool);
		bw.write(out);
		
		// 8. shorts
		ShortWritable sw = new ShortWritable();
		for (int i = 0; i < shrts.length; ++i)
		{
			sw.set(shrts[i]);
			sw.write(out);
		}
		sw.set(shrt);
		sw.write(out);
	}

	public void readFields(DataInput in) throws IOException
	{
		// 0. Timestamp
		Text tms_text = new Text(tms);
		tms_text.readFields(in);
		tms = tms_text.toString();


		// 1. integers
		IntWritable intw = new IntWritable();

		for (int i = 0; i < num.length; i++)
		{
			intw.readFields(in);
			num[i] = intw.get();
		}

		intw.readFields(in);
		int1 = intw.get();

		intw.readFields(in);
		int2 = intw.get();

		// 2. strings
		Text txt = new Text();

		for (int i = 0; i < strings.length; i++)
		{
			txt.readFields(in);
			strings[i] = txt.toString();
		}

		txt.readFields(in);
		st1 = txt.toString();

		// 3. doubles
		DoubleWritable dw = new DoubleWritable();
		for (int i = 0; i < dubs.length; i++)
		{
			dw.readFields(in);
			dubs[i] = dw.get();
		}

		dw.readFields(in);
		db = dw.get();

		// 4. floats
		FloatWritable fw = new FloatWritable();
		for (int i = 0; i < fts.length; i++)
		{
			fw.readFields(in);
			fts[i] = fw.get();
		}

		fw.readFields(in);
		ft = fw.get();

		// 5. longs
		LongWritable lw = new LongWritable();
		for (int i = 0; i < lngs.length; i++)
		{
			lw.readFields(in);
			lngs[i] = lw.get();
		}

		lw.readFields(in);
		lng = lw.get();

		// 6. bytes
		BytesWritable btsw = new BytesWritable();
		btsw.readFields(in);
		byte[] buffer = btsw.getBytes();
		bts = new byte[btsw.getLength()];
		for (int i = 0; i < btsw.getLength(); i++)
		{
			bts[i] = buffer[i];
		}

		// 7. booleans
		BooleanWritable bw = new BooleanWritable();
		for (int i = 0; i < bools.length; ++i)
		{
			bw.readFields(in);
			bools[i] = bw.get();
		}

		bw.readFields(in);
		bool = bw.get();
		
		// 8. shorts
		ShortWritable sw = new ShortWritable();
		for (int i = 0; i < shrts.length; ++i)
		{
			sw.readFields(in);
			shrts[i] = sw.get();
		}
		sw.readFields(in);
		shrt = sw.get();
	}

	public void printFieldTypes()
	{
		Class myClass = this.getClass();
		Field[] fields = myClass.getDeclaredFields();

		for (int i = 0; i < fields.length; i++) 
		{
			System.out.println(fields[i].getType().getName());
		}
	}
}	
