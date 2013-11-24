package com.pxf.infra.utils.fileformats.avro.schema;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

public class CustomAvroRecInSequence implements IAvroSchema {
	public String tms;
	public int[] num;
	public int int1;
	public int int2;
	public String[] strings;
	public String st1;
	public double[] dubs;
	public double db;
	public float[] fts;
	public float ft;
	public long[] lngs;
	public long lng;
	public byte[] bts;
	public boolean bl = true;

	// Avro variables
	private String schema_name;
	private Schema schema;
	private GenericRecord datum;
	private DatumWriter<GenericRecord> writer;
	private DatumReader<GenericRecord> reader;
	private EncoderFactory fct_en;

	private void initAvro() throws Exception {
		schema = new Schema.Parser().parse(new FileInputStream(schema_name));
		datum = new GenericData.Record(schema);
		writer = new GenericDatumWriter<GenericRecord>(schema);
		reader = new GenericDatumReader<GenericRecord>(schema);
		fct_en = EncoderFactory.get();
	}

	public CustomAvroRecInSequence(String parSchema) throws Exception {
		// 0.
		schema_name = parSchema;
		initAvro();

		// 1. Timestamp
		tms = new Timestamp(Calendar.getInstance().getTime().getTime()).toString();

		// 2. num array, int1, int2
		initNumArray();
		for (int i = 0; i < num.length; i++)
			num[i] = 0;

		int1 = 0;
		int2 = 0;

		// 3. Init strings
		initStringsArray();
		for (int i = 0; i < strings.length; i++)
			strings[i] = new String("");

		st1 = new String("");

		// 4. Init doubles
		initDoublesArray();
		for (int i = 0; i < dubs.length; i++)
			dubs[i] = 0.0;
		db = 0.0;

		// 5. Init floats
		initFloatsArray();
		for (int i = 0; i < fts.length; i++)
			fts[i] = 0.f;
		ft = 0.f;

		// 6. Init longs
		initLongsArray();
		for (int i = 0; i < lngs.length; i++)
			lngs[i] = 0;
		lng = 0;

		// 7. Init bytes
		initBytesArray();
		bts = "Sarkozy".getBytes();
	}

	public CustomAvroRecInSequence(String parSchema, Timestamp tm, int i1, int i2, int i3) throws Exception {
		// 0. Schema
		schema_name = parSchema;
		initAvro();

		// 1. Timestamp
		tms = tm.toString();

		// 2. num array, int1, int2
		initNumArray();
		for (int k = 0; k < num.length; k++)
			num[k] = i1 * 10 * (k + 1);

		int1 = i2;
		int2 = i3;

		// 3. Init strings
		initStringsArray();
		for (int k = 0; k < strings.length; k++)
			strings[k] = "strings_array_member_number___" + (k + 1);

		st1 = new String("short_string___" + i1);

		// 4. Init doubles
		initDoublesArray();
		for (int k = 0; k < dubs.length; k++)
			dubs[k] = i1 * 10.0 * (k + 1);
		db = (i1 + 5) * 10.0;

		// 5. Init floats
		initFloatsArray();
		for (int k = 0; k < fts.length; k++)
			fts[k] = i1 * 10.f * 2.3f * (k + 1);
		ft = i1 * 10.f * 2.3f;

		// 6. Init longs
		initLongsArray();
		for (int i = 0; i < lngs.length; i++)
			lngs[i] = i1 * 10 * (i + 3);
		lng = i1 * 10 + 5;

		// 7. Init bytes
		initBytesArray();
		bts = "Avro In Sequence !!!".getBytes();
	}

	void initNumArray() {
		num = new int[2];
	}

	void initStringsArray() {
		strings = new String[5];
	}

	void initDoublesArray() {
		dubs = new double[2];
	}

	void initFloatsArray() {
		fts = new float[2];
	}

	void initLongsArray() {
		lngs = new long[2];
	}

	void initBytesArray() {
		bts = new byte[10];
	}

	String GetTimestamp() {
		return tms;
	}

	int[] GetNum() {
		return num;
	}

	int GetInt1() {
		return int1;
	}

	int GetInt2() {
		return int2;
	}

	String[] GetStrings() {
		return strings;
	}

	String GetSt1() {
		return st1;
	}

	double[] GetDoubles() {
		return dubs;
	}

	double GetDb() {
		return db;
	}

	float[] GetFloats() {
		return fts;
	}

	float GetFt() {
		return ft;
	}

	long[] GetLongs() {
		return lngs;
	}

	long GetLong() {
		return lng;
	}

	byte[] GetBytes() {
		return bts;
	}

	@Override
	public void serialize(ByteArrayOutputStream out) throws IOException {
		// 0. Timestamp
		datum.put("tms", tms);

		// 1. num, int1, int2
		Schema.Field field = schema.getField("num");
		Schema fieldSchema = field.schema();
		GenericData.Array<Integer> intArray = new GenericData.Array<Integer>(num.length, fieldSchema);
		for (int i = 0; i < num.length; i++) {
			intArray.add(new Integer(num[i]));
		}
		datum.put("num", intArray);

		datum.put("int1", int1);
		datum.put("int2", int2);

		// 2. st1
		field = schema.getField("strings");
		fieldSchema = field.schema();
		GenericData.Array<Utf8> stringArray = new GenericData.Array<Utf8>(strings.length, fieldSchema);
		for (int i = 0; i < strings.length; i++) {
			stringArray.add(new Utf8(strings[i]));
		}
		datum.put("strings", stringArray);

		datum.put("st1", st1);

		// 3. doubles
		field = schema.getField("dubs");
		fieldSchema = field.schema();
		GenericData.Array<Double> doubleArray = new GenericData.Array<Double>(dubs.length, fieldSchema);
		for (int i = 0; i < dubs.length; i++) {
			doubleArray.add(new Double(dubs[i]));
		}
		datum.put("dubs", doubleArray);
		datum.put("db", db);

		// 4. floats
		field = schema.getField("fts");
		fieldSchema = field.schema();
		GenericData.Array<Float> floatArray = new GenericData.Array<Float>(fts.length, fieldSchema);
		for (int i = 0; i < fts.length; i++) {
			floatArray.add(new Float(fts[i]));
		}
		datum.put("fts", floatArray);
		datum.put("ft", ft);

		// 5. longs
		field = schema.getField("lngs");
		fieldSchema = field.schema();
		GenericData.Array<Long> longArray = new GenericData.Array<Long>(lngs.length, fieldSchema);
		for (int i = 0; i < lngs.length; i++) {
			longArray.add(lngs[i]);
		}
		datum.put("lngs", longArray);
		datum.put("lng", lng);

		// 6. bytes
		ByteBuffer byteBuffer = ByteBuffer.wrap(bts);
		datum.put("bts", byteBuffer);

		datum.put("bl", bl);

		Encoder encoder = fct_en.binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();
	}

	public void deserialize(byte[] bytes) throws IOException {
		Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
		GenericRecord record = reader.read(null, decoder);

		// 0. Timestamp
		tms = record.get("tms").toString();

		// 1. integers
		@SuppressWarnings("unchecked")
		GenericData.Array<Integer> intArray = (GenericData.Array<Integer>) record.get("num");
		for (int i = 0; i < intArray.size(); i++) {
			num[i] = intArray.get(i).intValue();
		}

		int1 = ((Integer) record.get("int1")).intValue();
		int2 = ((Integer) record.get("int2")).intValue();

		// 2. strings
		@SuppressWarnings("unchecked")
		GenericData.Array<Utf8> stringArray = (GenericData.Array<Utf8>) record.get("strings");
		for (int i = 0; i < stringArray.size(); i++) {
			strings[i] = stringArray.get(i).toString();
		}

		st1 = record.get("st1").toString();

		// 3. doubles
		@SuppressWarnings("unchecked")
		GenericData.Array<Double> doubleArray = (GenericData.Array<Double>) record.get("dubs");
		for (int i = 0; i < doubleArray.size(); i++) {
			dubs[i] = doubleArray.get(i).doubleValue();
		}

		db = ((Double) record.get("db")).doubleValue();

		// 4. floats
		@SuppressWarnings("unchecked")
		GenericData.Array<Float> floatArray = (GenericData.Array<Float>) record.get("fts");
		for (int i = 0; i < floatArray.size(); i++) {
			fts[i] = floatArray.get(i).floatValue();
		}

		ft = ((Float) record.get("ft")).floatValue();

		// 5. longs
		@SuppressWarnings("unchecked")
		GenericData.Array<Long> longArray = (GenericData.Array<Long>) record.get("lngs");
		for (int i = 0; i < longArray.size(); i++) {

			lngs[i] = longArray.get(i).longValue();
		}

		lng = ((Long) record.get("lng")).longValue();

		// 6. bytes
		ByteBuffer bytesBuffer = (ByteBuffer) record.get("bts");
		bts = bytesBuffer.array();

		bl = (Boolean) record.get("bl");
	}

	public void printFieldTypes() {
		Class myClass = this.getClass();
		Field[] fields = myClass.getDeclaredFields();

		for (int i = 0; i < fields.length; i++) {
			System.out.println(fields[i].getType().getName());
		}
	}

	@Override
	public GenericRecord serialize() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
