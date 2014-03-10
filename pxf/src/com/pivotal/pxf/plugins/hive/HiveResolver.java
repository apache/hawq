package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.*;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.pivotal.pxf.api.io.DataType.*;

/**
 * Class HiveResolver handles deserialization of records that were serialized 
 * using Hadoop's Hive serialization framework.  
 */

/*
TODO - remove SupressWarning once Hive resolves the problem described below
This line and the change of the deserialiazer member to Object instead of the original Deserializer...., All this changes stem from the same issue. 
In 0.11.0 The API changed and all Serde types extend a new interface - AbstractSerde. 
But this change was not adopted by the OrcSerde (which was also introduced in Hive 0.11.0). 
In order to cope with this inconsistency... this bit of juggling has been necessary.
*/
@SuppressWarnings("deprecation")
public class HiveResolver extends Plugin implements ReadResolver {
    private org.apache.hadoop.hive.serde2.SerDe deserializer;
    private List<OneField> partitionFields;
    private String serdeName;
    private String propsString;
    private String partitionKeys;
	
	/**
	 * Constructs the HiveResolver by parsing the userdata in the input 
	 * and obtaining the serde class name, the serde properties string 
	 * and the partition keys.
	 * @param input contains the Serde class name, the serde properties 
	 *        string and the partition keys
	 */
    public HiveResolver(InputData input) throws Exception {
        super(input);

        ParseUserData(input);
        InitSerde();
        InitPartitionFields();
    }
	
	public List<OneField> getFields(OneRow onerow) throws Exception {
        List<OneField> record = new LinkedList<OneField>();
		
        Object tuple = (deserializer).deserialize((Writable) onerow.getData());
        ObjectInspector oi = (deserializer).getObjectInspector();
		
        traverseTuple(tuple, oi, record);
        /* We follow Hive convention. Partition fields are always added at the end of the record */
        addPartitionKeyValues(record);
		
        return record;
    }	

    /*
     * parse user data string (arrived from fragmenter)
     */
    private void ParseUserData(InputData input) throws Exception {
        final int EXPECTED_NUM_OF_TOKS = 4;

        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HiveDataFragmenter.HIVE_UD_DELIM);

        if (toks.length != EXPECTED_NUM_OF_TOKS) {
            throw new UserDataException("HiveResolver expected " + EXPECTED_NUM_OF_TOKS + " tokens, but got " + toks.length);
        }

        serdeName = toks[1];
        propsString = toks[2];
        partitionKeys = toks[3];
    }

    /*
     * Get and init the deserializer for the records of this Hive data fragment
     */
    private void InitSerde() throws Exception {
        Properties serdeProperties;

        Class<?> c = Class.forName(serdeName, true, JavaUtils.getClassLoader());
        deserializer = (org.apache.hadoop.hive.serde2.SerDe) c.newInstance();
        serdeProperties = new Properties();
        ByteArrayInputStream inStream = new ByteArrayInputStream(propsString.getBytes());
        serdeProperties.load(inStream);
        deserializer.initialize(new JobConf(new Configuration(), HiveResolver.class), serdeProperties);
    }

    /*
     * The partition fields are initialized  one time  base on userData provided by the fragmenter
     */
    private void InitPartitionFields() {
        partitionFields = new LinkedList<OneField>();
        if (partitionKeys.compareTo(HiveDataFragmenter.HIVE_NO_PART_TBL) == 0) {
            return;
        }

        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
            String type = levelKey[1];
            String val = levelKey[2];

            switch (type) {
                case serdeConstants.STRING_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, TEXT, val);
                    break;
                case serdeConstants.SMALLINT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, SMALLINT, Short.parseShort(val));
                    break;
                case serdeConstants.INT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, INTEGER, Integer.parseInt(val));
                    break;
                case serdeConstants.BIGINT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, BIGINT, Long.parseLong(val));
                    break;
                case serdeConstants.FLOAT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, REAL, Float.parseFloat(val));
                    break;
                case serdeConstants.DOUBLE_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, FLOAT8, Double.parseDouble(val));
                    break;
                case serdeConstants.TIMESTAMP_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, TIMESTAMP, Timestamp.valueOf(val));
                    break;
                case serdeConstants.DECIMAL_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, NUMERIC, new HiveDecimal(val).bigDecimalValue().toString());
                    break;
                default:
                    throw new UnsupportedTypeException("Unknown type: " + type);
            }
        }
    }

    private void addPartitionKeyValues(List<OneField> record) {
        for (OneField field : partitionFields) {
            record.add(field);
        }
    }

    /*
     * If the object representing the whole record is null or if an object representing a composite sub-object (map, list,..)
     * is null - then BadRecordException will be thrown. If a primitive field value is null, then a null will appear for
     * the field in the record in the query result.
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector, List<OneField> record) throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        if ((obj == null) && (category != ObjectInspector.Category.PRIMITIVE)) {
            throw new BadRecordException("NULL Hive composite object");
        }
        List<?> list;
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector, record);
                break;
            case LIST:
                ListObjectInspector loi = (ListObjectInspector) objInspector;
                list = loi.getList(obj);
                ObjectInspector eoi = loi.getListElementObjectInspector();
                if (list == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type List");
                }
                for (Object object : list) {
                    traverseTuple(object, eoi, record);
                }
                break;
            case MAP:
                MapObjectInspector moi = (MapObjectInspector) objInspector;
                ObjectInspector koi = moi.getMapKeyObjectInspector();
                ObjectInspector voi = moi.getMapValueObjectInspector();
                Map<?, ?> map = moi.getMap(obj);
                if (map == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Map");
                }
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    traverseTuple(entry.getKey(), koi, record);
                    traverseTuple(entry.getValue(), voi, record);
                }
                break;
            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector) objInspector;
                List<? extends StructField> fields = soi.getAllStructFieldRefs();
                list = soi.getStructFieldsDataAsList(obj);
                if (list == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Struct");
                }
                for (int i = 0; i < list.size(); i++) {
                    traverseTuple(list.get(i), fields.get(i).getFieldObjectInspector(), record);
                }
                break;
            case UNION:
                UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
                List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
                if (ois == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Union");
                }
                traverseTuple(uoi.getField(obj), ois.get(uoi.getTag(obj)), record);
                break;
            default:
                throw new UnsupportedTypeException("Unknown category type: " + objInspector.getCategory());
        }
    }

    private void resolvePrimitive(Object o, PrimitiveObjectInspector oi, List<OneField> record) throws IOException {
        Object val;
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN: {
                val = (o != null) ? ((BooleanObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, BOOLEAN, val);
                break;
            }
            case SHORT: {
                val = (o != null) ? ((ShortObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, SMALLINT, val);
                break;
            }
            case INT: {
                val = (o != null) ? ((IntObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, INTEGER, val);
                break;
            }
            case LONG: {
                val = (o != null) ? ((LongObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, BIGINT, val);
                break;
            }
            case FLOAT: {
                val = (o != null) ? ((FloatObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, REAL, val);
                break;
            }
            case DOUBLE: {
                val = (o != null) ? ((DoubleObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, FLOAT8, val);
                break;
            }
            case DECIMAL: {
                String sVal = null;
                if (o != null) {
                    BigDecimal bd = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o).bigDecimalValue();
                    sVal = bd.toString();
                }
                addOneFieldToRecord(record, NUMERIC, sVal);
                break;
            }
            case STRING: {
                val = (o != null) ? ((StringObjectInspector) oi).getPrimitiveJavaObject(o) : null;
                addOneFieldToRecord(record, TEXT, val);
                break;
            }
            case BINARY: {
                byte[] toEncode = null;
                if (o != null) {
                    BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
                    toEncode = new byte[bw.getLength()];
                    System.arraycopy(bw.getBytes(), 0, toEncode, 0, bw.getLength());
                }
                addOneFieldToRecord(record, BYTEA, toEncode);
                break;
            }
            case TIMESTAMP: {
                val = (o != null) ? ((TimestampObjectInspector) oi).getPrimitiveJavaObject(o) : null;
                addOneFieldToRecord(record, TIMESTAMP, val);
                break;
            }
            default: {
                throw new UnsupportedTypeException(oi.getTypeName() + " conversion is not supported by " + getClass().getSimpleName());
            }
        }
    }

    void addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType, Object val) {
        OneField oneField = new OneField();
        oneField.type = gpdbWritableType.getOID();
        oneField.val = val;
        record.add(oneField);
    }
}
