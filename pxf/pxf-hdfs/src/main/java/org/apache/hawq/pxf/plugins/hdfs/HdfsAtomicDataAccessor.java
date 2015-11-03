package org.apache.hawq.pxf.plugins.hdfs;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Base class for enforcing the complete access of a file in one accessor.
 * Since we are not accessing the file using the splittable API, but instead are
 * using the "simple" stream API, it means that we cannot fetch different parts
 * (splits) of the file in different segments. Instead each file access brings
 * the complete file. And, if several segments would access the same file, then
 * each one will return the whole file and we will observe in the query result,
 * each record appearing number_of_segments times. To avoid this we will only
 * have one segment (segment 0) working for this case - enforced with
 * isWorkingSegment() method. Naturally this is the less recommended working
 * mode since we are not making use of segment parallelism. HDFS accessors for
 * a specific file type should inherit from this class only if the file they are
 * reading does not support splitting: a protocol-buffer file, regular file, ...
 */
public abstract class HdfsAtomicDataAccessor extends Plugin implements ReadAccessor {
    private Configuration conf = null;
    protected InputStream inp = null;
    private FileSplit fileSplit = null;

    /**
     * Constructs a HdfsAtomicDataAccessor object.
     *
     * @param input all input parameters coming from the client
     */
    public HdfsAtomicDataAccessor(InputData input) {
        // 0. Hold the configuration data
        super(input);

        // 1. Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
        conf = new Configuration();

        fileSplit = HdfsUtilities.parseFragmentMetadata(inputData);
    }

    /**
     * Opens the file using the non-splittable API for HADOOP HDFS file access
     * This means that instead of using a FileInputFormat for access, we use a
     * Java stream.
     *
     * @return true for successful file open, false otherwise
     */
    @Override
    public boolean openForRead() throws Exception {
        if (!isWorkingSegment()) {
            return false;
        }

        // input data stream
        FileSystem fs = FileSystem.get(URI.create(inputData.getDataSource()), conf); // FileSystem.get actually returns an FSDataInputStream
        inp = fs.open(new Path(inputData.getDataSource()));

        return (inp != null);
    }

    /**
     * Fetches one record from the file.
     *
     * @return a {@link OneRow} record as a Java object. Returns null if none.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        if (!isWorkingSegment()) {
            return null;
        }

        return new OneRow(null, new Object());
    }

    /**
     * Closes the access stream when finished reading the file
     */
    @Override
    public void closeForRead() throws Exception {
        if (!isWorkingSegment()) {
            return;
        }

        if (inp != null) {
            inp.close();
        }
    }

    /*
     * Making sure that only the segment that got assigned the first data
     * fragment will read the (whole) file.
     */
    private boolean isWorkingSegment() {
        return (fileSplit.getStart() == 0);
    }

    @Override
    public boolean isThreadSafe() {
        return HdfsUtilities.isThreadSafe(inputData.getDataSource(),
        								  inputData.getUserProperty("COMPRESSION_CODEC"));
    }
}
