package org.apache.hawq.pxf.plugins.hdfs;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Accessor for accessing a splittable HDFS data sources. HDFS will divide the
 * file into splits based on an internal decision (by default, the block size is
 * also the split size).
 *
 * Accessors that require such base functionality should extend this class.
 */
public abstract class HdfsSplittableDataAccessor extends Plugin implements
        ReadAccessor {
    protected Configuration conf = null;
    protected RecordReader<Object, Object> reader = null;
    protected InputFormat<?, ?> inputFormat = null;
    protected ListIterator<InputSplit> iter = null;
    protected JobConf jobConf = null;
    protected Object key, data;

    /**
     * Constructs an HdfsSplittableDataAccessor
     *
     * @param input all input parameters coming from the client request
     * @param inFormat the HDFS {@link InputFormat} the caller wants to use
     */
    public HdfsSplittableDataAccessor(InputData input,
                                      InputFormat<?, ?> inFormat) {
        super(input);
        inputFormat = inFormat;

        // 1. Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
        conf = new Configuration();

        // 2. variable required for the splits iteration logic
        jobConf = new JobConf(conf, HdfsSplittableDataAccessor.class);
    }

    /**
     * Fetches the requested fragment (file split) for the current client
     * request, and sets a record reader for the job.
     *
     * @return true if succeeded, false if no more splits to be read
     */
    @Override
    public boolean openForRead() throws Exception {
        LinkedList<InputSplit> requestSplits = new LinkedList<InputSplit>();
        FileSplit fileSplit = HdfsUtilities.parseFragmentMetadata(inputData);
        requestSplits.add(fileSplit);

        // Initialize record reader based on current split
        iter = requestSplits.listIterator(0);
        return getNextSplit();
    }

    /**
     * Specialized accessors will override this method and implement their own
     * recordReader. For example, a plain delimited text accessor may want to
     * return a LineRecordReader.
     *
     * @param jobConf the hadoop jobconf to use for the selected InputFormat
     * @param split the input split to be read by the accessor
     * @return a recordreader to be used for reading the data records of the
     *         split
     * @throws IOException if recordreader could not be created
     */
    abstract protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException;

    /**
     * Sets the current split and initializes a RecordReader who feeds from the
     * split
     *
     * @return true if there is a split to read
     * @throws IOException if record reader could not be created
     */
    @SuppressWarnings(value = "unchecked")
    protected boolean getNextSplit() throws IOException  {
        if (!iter.hasNext()) {
            return false;
        }

        InputSplit currSplit = iter.next();
        reader = (RecordReader<Object, Object>) getReader(jobConf, currSplit);
        key = reader.createKey();
        data = reader.createValue();
        return true;
    }

    /**
     * Fetches one record from the file. The record is returned as a Java
     * object.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        // if there is one more record in the current split
        if (!reader.next(key, data)) {
            // the current split is exhausted. try to move to the next split
            if (getNextSplit()) {
                // read the first record of the new split
                if (!reader.next(key, data)) {
                    // make sure we return nulls
                    return null;
                }
            } else {
                // make sure we return nulls
                return null;
            }
        }

        /*
         * if neither condition was met, it means we already read all the
         * records in all the splits, and in this call record variable was not
         * set, so we return null and thus we are signaling end of records
         * sequence
         */
        return new OneRow(key, data);
    }

    /**
     * When user finished reading the file, it closes the RecordReader
     */
    @Override
    public void closeForRead() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public boolean isThreadSafe() {
        return HdfsUtilities.isThreadSafe(inputData.getDataSource(),
                inputData.getUserProperty("COMPRESSION_CODEC"));
    }

}
