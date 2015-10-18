package org.apache.hawq.pxf.service;

import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.service.io.BufferWritable;
import org.apache.hawq.pxf.service.io.GPDBWritable;
import org.apache.hawq.pxf.service.io.GPDBWritable.TypeMismatchException;
import org.apache.hawq.pxf.service.io.Text;
import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.ProtocolData;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hawq.pxf.api.io.DataType.TEXT;

/**
 * Class creates the output record that is piped by the java process to the HAWQ
 * backend. Actually, the output record is serialized and the obtained byte
 * string is piped to the HAWQ segment. The output record will implement
 * Writable, and the mission of BridgeOutputBuilder will be to translate a list
 * of {@link OneField} objects (obtained from the Resolver) into an output
 * record.
 */
public class BridgeOutputBuilder {
    private ProtocolData inputData;
    private Writable output = null;
    private LinkedList<Writable> outputList = null;
    private Writable partialLine = null;
    private GPDBWritable errorRecord = null;
    private int[] schema;
    private String[] colNames;
    private boolean samplingEnabled = false;
    private boolean isPartialLine = false;

    private static final byte DELIM = 10; /* (byte)'\n'; */

    private static final Log LOG = LogFactory.getLog(BridgeOutputBuilder.class);

    /**
     * Constructs a BridgeOutputBuilder.
     *
     * @param input input data, like requested output format and schema
     *            information
     */
    public BridgeOutputBuilder(ProtocolData input) {
        inputData = input;
        outputList = new LinkedList<Writable>();
        makeErrorRecord();
        samplingEnabled = (inputData.getStatsSampleRatio() > 0);
    }

    /**
     * We need a separate GPDBWritable record to represent the error record.
     * Just setting the errorFlag on the "output" GPDBWritable variable is not
     * good enough, since the GPDBWritable is built only after the first record
     * is read from the file. And if we encounter an error while fetching the
     * first record from the file, then the output member will be null. The
     * reason we cannot count on the schema to build the GPDBWritable output
     * variable before reading the first record, is because the schema does not
     * account for arrays - we cannot know from the schema the length of an
     * array. We find out only after fetching the first record.
     */
    void makeErrorRecord() {
        int[] errSchema = { TEXT.getOID() };

        if (inputData.outputFormat() != OutputFormat.BINARY) {
            return;
        }

        errorRecord = new GPDBWritable(errSchema);
        errorRecord.setError(true);
    }

    /**
     * Returns the error record. If the output format is not binary, error
     * records are not supported, and the given exception will be thrown
     *
     * @param ex exception to be stored in record
     * @return error record
     * @throws Exception if the output format is not binary
     */
    public Writable getErrorOutput(Exception ex) throws Exception {
        if (inputData.outputFormat() == OutputFormat.BINARY) {
            errorRecord.setString(0, ex.getMessage());
            return errorRecord;
        } else {
            throw ex;
        }
    }

    /**
     * Translates recFields (obtained from the Resolver) into an output record.
     *
     * @param recFields record fields to be serialized
     * @return list of Writable objects with serialized row
     * @throws BadRecordException if building the output record failed
     */
    public LinkedList<Writable> makeOutput(List<OneField> recFields)
            throws BadRecordException {
        if (output == null && inputData.outputFormat() == OutputFormat.BINARY) {
            makeGPDBWritableOutput();
        }

        outputList.clear();

        fillOutputRecord(recFields);

        return outputList;
    }

    /**
     * Returns whether or not this is a partial line.
     *
     * @return true for a partial line
     */
    public Writable getPartialLine() {
        return partialLine;
    }

    /**
     * Creates the GPDBWritable object. The object is created one time and is
     * refilled from recFields for each record sent
     *
     * @return empty GPDBWritable object with set columns
     */
    GPDBWritable makeGPDBWritableOutput() {
        int num_actual_fields = inputData.getColumns();
        schema = new int[num_actual_fields];
        colNames = new String[num_actual_fields];

        for (int i = 0; i < num_actual_fields; i++) {
            schema[i] = inputData.getColumn(i).columnTypeCode();
            colNames[i] = inputData.getColumn(i).columnName();
        }

        output = new GPDBWritable(schema);

        return (GPDBWritable) output;
    }

    /**
     * Fills the output record based on the fields in recFields.
     *
     * @param recFields record fields
     * @throws BadRecordException if building the output record failed
     */
    void fillOutputRecord(List<OneField> recFields) throws BadRecordException {
        if (inputData.outputFormat() == OutputFormat.BINARY) {
            fillGPDBWritable(recFields);
        } else {
            fillText(recFields);
        }
    }

    /**
     * Fills a GPDBWritable object based on recFields. The input record
     * recFields must correspond to schema. If the record has more or less
     * fields than the schema we throw an exception. We require that the type of
     * field[i] in recFields corresponds to the type of field[i] in the schema.
     *
     * @param recFields record fields
     * @throws BadRecordException if building the output record failed
     */
    void fillGPDBWritable(List<OneField> recFields) throws BadRecordException {
        int size = recFields.size();
        if (size == 0) { // size 0 means the resolver couldn't deserialize any
                         // of the record fields
            throw new BadRecordException("No fields in record");
        } else if (size != schema.length) {
            throw new BadRecordException("Record has " + size
                    + " fields but the schema size is " + schema.length);
        }

        for (int i = 0; i < size; i++) {
            OneField current = recFields.get(i);
            if (!isTypeInSchema(current.type, schema[i])) {
                throw new BadRecordException("For field " + colNames[i]
                        + " schema requires type "
                        + DataType.get(schema[i]).toString()
                        + " but input record has type "
                        + DataType.get(current.type).toString());
            }

            fillOneGPDBWritableField(current, i);
        }

        outputList.add(output);
    }

    /**
     * Tests if data type is a string type. String type is a type that can be
     * serialized as string, such as varchar, bpchar, text, numeric, timestamp,
     * date.
     *
     * @param type data type
     * @return whether data type is string type
     */
    boolean isStringType(DataType type) {
        return Arrays.asList(DataType.VARCHAR, DataType.BPCHAR, DataType.TEXT,
                DataType.NUMERIC, DataType.TIMESTAMP, DataType.DATE).contains(
                type);
    }

    /**
     * Tests if record field type and schema type correspond.
     *
     * @param recType record type code
     * @param schemaType schema type code
     * @return whether record type and schema type match
     */
    boolean isTypeInSchema(int recType, int schemaType) {
        DataType dtRec = DataType.get(recType);
        DataType dtSchema = DataType.get(schemaType);

        return (dtSchema == DataType.UNSUPPORTED_TYPE || dtRec == dtSchema || (isStringType(dtRec) && isStringType(dtSchema)));
    }

    /**
     * Fills a Text object based on recFields.
     *
     * @param recFields record fields
     * @throws BadRecordException if text formatted record has more than one
     *             field
     */
    void fillText(List<OneField> recFields) throws BadRecordException {
        /*
         * For the TEXT case there must be only one record in the list
         */
        if (recFields.size() != 1) {
            throw new BadRecordException(
                    "BridgeOutputBuilder must receive one field when handling the TEXT format");
        }

        OneField fld = recFields.get(0);
        int type = fld.type;
        Object val = fld.val;
        if (DataType.get(type) == DataType.BYTEA) {// from LineBreakAccessor
            if (samplingEnabled) {
                convertTextDataToLines((byte[]) val);
            } else {
                output = new BufferWritable((byte[]) val);
                outputList.add(output); // TODO break output into lines
            }
        } else { // from QuotedLineBreakAccessor
            String textRec = (String) val;
            output = new Text(textRec + "\n");
            outputList.add(output);
        }
    }

    void convertTextDataToLines(byte[] val) {
        int len = val.length;
        int start = 0;
        int end = 0;
        byte[] line;
        BufferWritable writable;

        while (start < len) {
            end = ArrayUtils.indexOf(val, DELIM, start);
            if (end == ArrayUtils.INDEX_NOT_FOUND) {
                // data finished in the middle of the line
                end = len;
                isPartialLine = true;
            } else {
                end++; // include the DELIM character
                isPartialLine = false;
            }
            line = Arrays.copyOfRange(val, start, end);

            if (partialLine != null) {
                // partial data was completed
                ((BufferWritable) partialLine).append(line);
                writable = (BufferWritable) partialLine;
                partialLine = null;
            } else {
                writable = new BufferWritable(line);
            }

            if (isPartialLine) {
                partialLine = writable;
            } else {
                outputList.add(writable);
            }
            start = end;
        }
    }

    /**
     * Fills one GPDBWritable field.
     *
     * @param oneField field
     * @param colIdx column index
     * @throws BadRecordException if field type is not supported or doesn't
     *             match the schema
     */
    void fillOneGPDBWritableField(OneField oneField, int colIdx)
            throws BadRecordException {
        int type = oneField.type;
        Object val = oneField.val;
        GPDBWritable gpdbOutput = (GPDBWritable) output;
        try {
            switch (DataType.get(type)) {
                case INTEGER:
                    gpdbOutput.setInt(colIdx, (Integer) val);
                    break;
                case FLOAT8:
                    gpdbOutput.setDouble(colIdx, (Double) val);
                    break;
                case REAL:
                    gpdbOutput.setFloat(colIdx, (Float) val);
                    break;
                case BIGINT:
                    gpdbOutput.setLong(colIdx, (Long) val);
                    break;
                case SMALLINT:
                    gpdbOutput.setShort(colIdx, (Short) val);
                    break;
                case BOOLEAN:
                    gpdbOutput.setBoolean(colIdx, (Boolean) val);
                    break;
                case BYTEA:
                    byte[] bts = null;
                    if (val != null) {
                        int length = Array.getLength(val);
                        bts = new byte[length];
                        for (int j = 0; j < length; j++) {
                            bts[j] = Array.getByte(val, j);
                        }
                    }
                    gpdbOutput.setBytes(colIdx, bts);
                    break;
                case VARCHAR:
                case BPCHAR:
                case CHAR:
                case TEXT:
                case NUMERIC:
                case TIMESTAMP:
                case DATE:
                    gpdbOutput.setString(colIdx,
                            ObjectUtils.toString(val, null));
                    break;
                default:
                    String valClassName = (val != null) ? val.getClass().getSimpleName()
                            : null;
                    throw new UnsupportedOperationException(valClassName
                            + " is not supported for HAWQ conversion");
            }
        } catch (TypeMismatchException e) {
            throw new BadRecordException(e);
        }
    }
}
