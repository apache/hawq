package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.WriteAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * A PXF Accessor for reading delimited plain text records
 */
public class LineBreakAccessor extends HdfsSplittableDataAccessor implements WriteAccessor {
    private DataOutputStream dos;
    private FSDataOutputStream fsdos;
    private Configuration conf;
    private FileSystem fs;
    private Path file;
    private Log Log;

    /**
     * Constructs a LineReaderAccessor
     * 
     * @param input all input parameters coming from the client request
     * @throws Exception
     */
    public LineBreakAccessor(InputData input) throws Exception {
        super(input, new TextInputFormat());
        ((TextInputFormat) inputFormat).configure(jobConf);

        Log = LogFactory.getLog(LineBreakAccessor.class);
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new ChunkRecordReader(jobConf, (FileSplit) split);
    }
	
    /* 
     * opens file for write
     */
    public boolean openForWrite() throws Exception {

        String fileName = inputData.getDataSource();
        String compressCodec = inputData.getUserProperty("COMPRESSION_CODEC");
        CompressionCodec codec = null;

        conf = new Configuration();
        fs = FileSystem.get(conf);

        // get compression codec
        if (compressCodec != null) {
            codec = HdfsUtilities.getCodec(conf, compressCodec);
            String extension = codec.getDefaultExtension();
            fileName += extension;
        }

        file = new Path(fileName);

        if (fs.exists(file)) {
            throw new IOException("file " + file.toString() + " already exists, can't write data");
        }
        org.apache.hadoop.fs.Path parent = file.getParent();
        if (!fs.exists(parent)) {
            fs.mkdirs(parent);
            Log.debug("Created new dir " + parent.toString());
        }

        // create output stream - do not allow overwriting existing file
        createOutputStream(file, codec);

        return true;
    }

    /*
     * Create output stream from given file.
     * If compression codec is provided, wrap it around stream.
     */
    private void createOutputStream(Path file, CompressionCodec codec) throws IOException {
        fsdos = fs.create(file, false);
        if (codec != null) {
            dos = new DataOutputStream(codec.createOutputStream(fsdos));
        } else {
            dos = fsdos;
        }

    }

    /*
     * write row into stream
     */
    public boolean writeNextObject(OneRow onerow) throws Exception {
        dos.write((byte[]) onerow.getData());
        return true;
    }
    
    /* 
     * close the output stream after done writing
     */
    public void closeForWrite() throws Exception {
        if ((dos != null) && (fsdos != null)) {
            Log.debug("Closing writing stream for path " + file);
            dos.flush();
            /*
             * From release 0.21.0 sync() is deprecated in favor of hflush(),
			 * which only guarantees that new readers will see all data written 
			 * to that point, and hsync(), which makes a stronger guarantee that
			 * the operating system has flushed the data to disk (like POSIX 
			 * fsync), although data may still be in the disk cache.
			 */
            fsdos.hsync();
            dos.close();
        }
    }
}
