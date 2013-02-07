package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;
import java.io.ByteArrayInputStream;
import java.lang.IllegalAccessException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

/*
 * Class HiveResolver handles deserialization of records that were serialized 
 * using Hadoop's Hive serialization framework. HiveResolver implements
 * IFieldsResolver exposing one method: GetFields
 */
class HiveResolver implements  IFieldsResolver
{
    private HDFSMetaData connectorConfiguration;
	private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();

	// reflection fields
	private Object userObject = null;
	private int index_of_readFields = 0;
	////////////////////////////
	private Deserializer deserializer;
	private Properties serdeProperties;
	

	public HiveResolver(HDFSMetaData conf) throws Exception
	{
        connectorConfiguration = conf;
		String userData = connectorConfiguration.getProperty("X-GP-FRAGMENT-USER-DATA");
		String[] toks = userData.split(HiveDataFragmenter.HIVE_USER_DATA_DELIM);		
		
		String serdeName = toks[1]; /* look in HiveDataFragmenter to see how the userData was concatanated */
		Class<?> c = Class.forName(serdeName, true, JavaUtils.getClassLoader());
		deserializer = (Deserializer)c.newInstance();
		String propsString = toks[2];
		ByteArrayInputStream inStream = new ByteArrayInputStream(propsString.getBytes());
		serdeProperties = new Properties();
		serdeProperties.load(inStream);
		
		deserializer.initialize(new JobConf(new Configuration(), HiveResolver.class), serdeProperties);
	}

	/*
	 * GetFields returns a list of the fields of one record.
	 * Each record field is represented by a OneField item.
	 * OneField item contains two fields: an integer representing the field type and a Java
	 * Object representing the field value.
	 */
	public List<OneField> GetFields(OneRow onerow) throws Exception
	{
		List<OneField> record =  new LinkedList<OneField>();
		
		Object tuple = deserializer.deserialize((Writable)onerow.getData());
		ObjectInspector oi = deserializer.getObjectInspector();
		
		traverseTuple(tuple, oi, record);
		return record;
	}
	
	public void traverseTuple(Object obj, ObjectInspector objInspector, List<OneField> record) throws IOException
	{
		
		if (obj == null) 
			return;
		
		List<?> list;
		switch (objInspector.getCategory()) 
		{
			case PRIMITIVE:
				resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector, record);
				return;
			case LIST:
				ListObjectInspector loi = (ListObjectInspector) objInspector;
				list = loi.getList(obj);
				ObjectInspector eoi = loi.getListElementObjectInspector();
				if (list == null)
					return;
				else 
				{
					for (int i = 0; i < list.size(); i++) 
						traverseTuple(list.get(i), eoi, record);
				}
				return;
			case MAP:
				MapObjectInspector moi = (MapObjectInspector) objInspector;
				ObjectInspector koi = moi.getMapKeyObjectInspector();
				ObjectInspector voi = moi.getMapValueObjectInspector();
				Map<?, ?> map = moi.getMap(obj);
				if (map == null)
					return;
				else 
				{
					for (Map.Entry<?, ?> entry : map.entrySet()) 
					{
						traverseTuple(entry.getKey(), koi, record);
						traverseTuple(entry.getValue(), voi, record);
					}
				}
				return;
			case STRUCT:
				StructObjectInspector soi = (StructObjectInspector) objInspector;
				List<? extends StructField> fields = soi.getAllStructFieldRefs();
				list = soi.getStructFieldsDataAsList(obj);
				if (list == null)
					return;
				else 
				{
					for (int i = 0; i < list.size(); i++) 
						traverseTuple(list.get(i), fields.get(i).getFieldObjectInspector(), record);
				}
				return;
			case UNION:
				UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
				List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
				if (ois == null)
					return;
				else 
					traverseTuple(uoi.getField(obj), ois.get(uoi.getTag(obj)), record);
				return;
			default:
				break;
		}
		
		throw new RuntimeException("Unknown category type: "
								   + objInspector.getCategory());
	}
	
	public void resolvePrimitive(Object o, PrimitiveObjectInspector oi, List<OneField> record) throws IOException
	{
		switch (oi.getPrimitiveCategory()) 
		{
			case BOOLEAN: 
			{
				boolean b = ((BooleanObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.BOOLEAN, b);
				break;
			}
			case BYTE: {
				byte b = ((ByteObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.CHAR, b);
				break;
			}
			case SHORT: {
				short s = ((ShortObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.SMALLINT, s);
				break;
			}
			case INT: {
				int i = ((IntObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.INTEGER, i);
				break;
			}
			case LONG: {
				long l = ((LongObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.BIGINT, l);
				break;
			}
			case FLOAT: {
				float f = ((FloatObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.REAL, f);
				break;
			}
			case DOUBLE: {
				double d = ((DoubleObjectInspector) oi).get(o);
				addOneFieldToRecord(record, GPDBWritable.FLOAT8, d);
				break;
			}
			case STRING: {
				String s = ((StringObjectInspector) oi).getPrimitiveJavaObject(o);
				addOneFieldToRecord(record, GPDBWritable.TEXT, s);
				break;
			}
				
			case BINARY: {
				BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
				byte[] toEncode = new byte[bw.getLength()];
				System.arraycopy(bw.getBytes(), 0,toEncode, 0, bw.getLength());
				addOneFieldToRecord(record, GPDBWritable.BYTEA, toEncode);
				break;
			}
			case TIMESTAMP: {
				Timestamp t = ((TimestampObjectInspector) oi).getPrimitiveJavaObject(o);
				addOneFieldToRecord(record, GPDBWritable.TIMESTAMP, t);
				break;
			}
			default: {
				throw new RuntimeException("Unknown type: " + oi.getPrimitiveCategory().toString());
			}
		}		
	}

	void addOneFieldToRecord(List<OneField> record, int gpdbWritableType, Object val)
	{
		OneField oneField = new OneField();

		oneField.type = gpdbWritableType;
		oneField.val = val;
		record.add(oneField);
	}
}
