package org.apache.hawq.pxf.plugins.hdfs;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.utilities.InputData;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

import static org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities.getAvroSchema;

/**
 * A PXF Accessor for reading Avro File records
 */
public class AvroFileAccessor extends HdfsSplittableDataAccessor {
    private AvroWrapper<GenericRecord> avroWrapper = null;

    /**
     * Constructs a AvroFileAccessor that creates the job configuration and
     * accesses the avro file to fetch the avro schema
     *
     * @param input all input parameters coming from the client
     * @throws Exception if getting the avro schema fails
     */
    public AvroFileAccessor(InputData input) throws Exception {
        // 1. Call the base class
        super(input, new AvroInputFormat<GenericRecord>());

        // 2. Accessing the avro file through the "unsplittable" API just to get the schema.
        //    The splittable API (AvroInputFormat) which is the one we will be using to fetch
        //    the records, does not support getting the avro schema yet.
        Schema schema = getAvroSchema(conf, inputData.getDataSource());

        // 3. Pass the schema to the AvroInputFormat
        AvroJob.setInputSchema(jobConf, schema);

        // 4. The avroWrapper required for the iteration
        avroWrapper = new AvroWrapper<GenericRecord>();
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new AvroRecordReader<Object>(jobConf, (FileSplit) split);
    }

    /**
     * readNextObject
     * The AVRO accessor is currently the only specialized accessor that
     * overrides this method. This happens, because the special
     * AvroRecordReader.next() semantics (use of the AvroWrapper), so it
     * cannot use the RecordReader's default implementation in
     * SplittableFileAccessor
     */
    @Override
    public OneRow readNextObject() throws IOException {
        /** Resetting datum to null, to avoid stale bytes to be padded from the previous row's datum */
        avroWrapper.datum(null);
        if (reader.next(avroWrapper, NullWritable.get())) { // There is one more record in the current split.
            return new OneRow(null, avroWrapper.datum());
        } else if (getNextSplit()) { // The current split is exhausted. try to move to the next split.
            return reader.next(avroWrapper, NullWritable.get())
                    ? new OneRow(null, avroWrapper.datum())
                    : null;
        }

        // if neither condition was met, it means we already read all the records in all the splits, and
        // in this call record variable was not set, so we return null and thus we are signaling end of
        // records sequence - in this case avroWrapper.datum() will be null
        return null;
    }
}
