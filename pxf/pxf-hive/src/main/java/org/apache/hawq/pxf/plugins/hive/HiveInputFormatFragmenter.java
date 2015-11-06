package org.apache.hawq.pxf.plugins.hive;

import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Arrays;
import java.util.List;

/**
 * Specialized Hive fragmenter for RC and Text files tables. Unlike the
 * {@link HiveDataFragmenter}, this class does not send the serde properties to
 * the accessor/resolvers. This is done to avoid memory explosion in Hawq. For
 * RC use together with {@link HiveRCFileAccessor}/
 * {@link HiveColumnarSerdeResolver}. For Text use together with
 * {@link HiveLineBreakAccessor}/{@link HiveStringPassResolver}. <br>
 * Given a Hive table and its partitions, divide the data into fragments (here a
 * data fragment is actually a HDFS file block) and return a list of them. Each
 * data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is
 * part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData: inputformat name, serde names and partition keys</li>
 * </ol>
 */
public class HiveInputFormatFragmenter extends HiveDataFragmenter {
    private static final Log LOG = LogFactory.getLog(HiveInputFormatFragmenter.class);

    static final String STR_RC_FILE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    static final String STR_TEXT_FILE_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    static final String STR_COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
    static final String STR_LAZY_BINARY_COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe";
    static final String STR_LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final int EXPECTED_NUM_OF_TOKS = 3;
    public static final int TOK_SERDE = 0;
    public static final int TOK_KEYS = 1;
    public static final int TOK_FILTER_DONE = 2;

    /** Defines the Hive input formats currently supported in pxf */
    public enum PXF_HIVE_INPUT_FORMATS {
        RC_FILE_INPUT_FORMAT,
        TEXT_FILE_INPUT_FORMAT
    }

    /** Defines the Hive serializers (serde classes) currently supported in pxf */
    public enum PXF_HIVE_SERDES {
        COLUMNAR_SERDE,
        LAZY_BINARY_COLUMNAR_SERDE,
        LAZY_SIMPLE_SERDE
    }

    /**
     * Constructs a HiveInputFormatFragmenter.
     *
     * @param inputData all input parameters coming from the client
     */
    public HiveInputFormatFragmenter(InputData inputData) {
        super(inputData, HiveInputFormatFragmenter.class);
    }

    /**
     * Extracts the user data:
     * serde, partition keys and whether filter was included in fragmenter
     *
     * @param input input data from client
     * @param supportedSerdes supported serde names
     * @return parsed tokens
     * @throws UserDataException if user data contains unsupported serde
     *                           or wrong number of tokens
     */
    static public String[] parseToks(InputData input, String... supportedSerdes)
            throws UserDataException {
        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HIVE_UD_DELIM);
        if (supportedSerdes.length > 0
                && !Arrays.asList(supportedSerdes).contains(toks[TOK_SERDE])) {
            throw new UserDataException(toks[TOK_SERDE]
                    + " serializer isn't supported by " + input.getAccessor());
        }

        if (toks.length != (EXPECTED_NUM_OF_TOKS)) {
            throw new UserDataException("HiveInputFormatFragmenter expected "
                    + EXPECTED_NUM_OF_TOKS + " tokens, but got " + toks.length);
        }

        return toks;
    }

    /*
     * Checks that hive fields and partitions match the HAWQ schema. Throws an
     * exception if: - the number of fields (+ partitions) do not match the HAWQ
     * table definition. - the hive fields types do not match the HAWQ fields.
     */
    @Override
    void verifySchema(Table tbl) throws Exception {

        int columnsSize = inputData.getColumns();
        int hiveColumnsSize = tbl.getSd().getColsSize();
        int hivePartitionsSize = tbl.getPartitionKeysSize();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Hive table: " + hiveColumnsSize + " fields, "
                    + hivePartitionsSize + " partitions. " + "HAWQ table: "
                    + columnsSize + " fields.");
        }

        // check schema size
        if (columnsSize != (hiveColumnsSize + hivePartitionsSize)) {
            throw new IllegalArgumentException("Hive table schema ("
                    + hiveColumnsSize + " fields, " + hivePartitionsSize
                    + " partitions) " + "doesn't match PXF table ("
                    + columnsSize + " fields)");
        }

        int index = 0;
        // check hive fields
        List<FieldSchema> hiveColumns = tbl.getSd().getCols();
        for (FieldSchema hiveCol : hiveColumns) {
            ColumnDescriptor colDesc = inputData.getColumn(index++);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            compareTypes(colType, hiveCol.getType(), colDesc.columnName());
        }
        // check partition fields
        List<FieldSchema> hivePartitions = tbl.getPartitionKeys();
        for (FieldSchema hivePart : hivePartitions) {
            ColumnDescriptor colDesc = inputData.getColumn(index++);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            compareTypes(colType, hivePart.getType(), colDesc.columnName());
        }

    }

    private void compareTypes(DataType type, String hiveType, String fieldName) {
        String convertedHive = toHiveType(type, fieldName);
        if (!convertedHive.equals(hiveType)
                && !(convertedHive.equals("smallint") && hiveType.equals("tinyint"))) {
            throw new UnsupportedTypeException(
                    "Schema mismatch definition: Field " + fieldName
                            + " (Hive type " + hiveType + ", HAWQ type "
                            + type.toString() + ")");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Field " + fieldName + ": Hive type " + hiveType
                    + ", HAWQ type " + type.toString());
        }
    }

    /**
     * Converts HAWQ type to hive type. The supported mappings are:<ul>
     * <li>{@code BOOLEAN -> boolean}</li>
     * <li>{@code SMALLINT -> smallint (tinyint is converted to smallint)}</li>
     * <li>{@code BIGINT -> bigint}</li>
     * <li>{@code TIMESTAMP, TIME -> timestamp}</li>
     * <li>{@code NUMERIC -> decimal}</li>
     * <li>{@code BYTEA -> binary}</li>
     * <li>{@code INTERGER -> int}</li>
     * <li>{@code TEXT -> string}</li>
     * <li>{@code REAL -> float}</li>
     * <li>{@code FLOAT8 -> double}</li>
     * </ul>
     * All other types (both in HAWQ and in HIVE) are not supported.
     *
     * @param type HAWQ data type
     * @param name field name
     * @return Hive type
     * @throws UnsupportedTypeException if type is not supported
     */
    public static String toHiveType(DataType type, String name) {
        switch (type) {
            case BOOLEAN:
            case SMALLINT:
            case BIGINT:
            case TIMESTAMP:
                return type.toString().toLowerCase();
            case NUMERIC:
                return "decimal";
            case BYTEA:
                return "binary";
            case INTEGER:
                return "int";
            case TEXT:
                return "string";
            case REAL:
                return "float";
            case FLOAT8:
                return "double";
            case TIME:
                return "timestamp";
            default:
                throw new UnsupportedTypeException(
                        type.toString()
                                + " conversion is not supported by HiveInputFormatFragmenter (Field "
                                + name + ")");
        }
    }

    /*
     * Validates that partition format corresponds to PXF supported formats and
     * transforms the class name to an enumeration for writing it to the
     * accessors on other PXF instances.
     */
    private String assertFileType(String className, HiveTablePartition partData)
            throws Exception {
        switch (className) {
            case STR_RC_FILE_INPUT_FORMAT:
                return PXF_HIVE_INPUT_FORMATS.RC_FILE_INPUT_FORMAT.name();
            case STR_TEXT_FILE_INPUT_FORMAT:
                return PXF_HIVE_INPUT_FORMATS.TEXT_FILE_INPUT_FORMAT.name();
            default:
                throw new IllegalArgumentException(
                        "HiveInputFormatFragmenter does not yet support "
                                + className
                                + " for "
                                + partData
                                + ". Supported InputFormat are "
                                + Arrays.toString(PXF_HIVE_INPUT_FORMATS.values()));
        }
    }

    /*
     * Validates that partition serde corresponds to PXF supported serdes and
     * transforms the class name to an enumeration for writing it to the
     * resolvers on other PXF instances.
     */
    private String assertSerde(String className, HiveTablePartition partData)
            throws Exception {
        switch (className) {
            case STR_COLUMNAR_SERDE:
                return PXF_HIVE_SERDES.COLUMNAR_SERDE.name();
            case STR_LAZY_BINARY_COLUMNAR_SERDE:
                return PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE.name();
            case STR_LAZY_SIMPLE_SERDE:
                return PXF_HIVE_SERDES.LAZY_SIMPLE_SERDE.name();
            default:
                throw new UnsupportedTypeException(
                        "HiveInputFormatFragmenter does not yet support  "
                                + className + " for " + partData
                                + ". Supported serializers are: "
                                + Arrays.toString(PXF_HIVE_SERDES.values()));
        }
    }

    @Override
    byte[] makeUserData(HiveTablePartition partData) throws Exception {
        String inputFormatName = partData.storageDesc.getInputFormat();
        String serdeName = partData.storageDesc.getSerdeInfo().getSerializationLib();
        String partitionKeys = serializePartitionKeys(partData);

        assertFileType(inputFormatName, partData);
        String userData = assertSerde(serdeName, partData) + HIVE_UD_DELIM
                + partitionKeys + HIVE_UD_DELIM + filterInFragmenter;

        return userData.getBytes();
    }

    /**
     * Returns statistics for Hive table. Currently it's not implemented.
     */
    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        throw new UnsupportedOperationException("ANALYZE for HiveRc and HiveText plugins is not supported");
    }
}
