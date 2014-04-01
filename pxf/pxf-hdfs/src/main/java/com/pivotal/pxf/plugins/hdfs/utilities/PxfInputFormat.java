package com.pivotal.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * PxfInputFormat is not intended to read a specific format, hence it implements
 * a dummy getRecordReader Instead, its purpose is to apply
 * FileInputFormat.getSplits from one point in PXF and get the splits which are
 * valid for the actual InputFormats, since all of them we use inherit
 * FileInputFormat but do not override getSplits.
 */
public class PxfInputFormat extends FileInputFormat {

    @Override
    public RecordReader getRecordReader(InputSplit split,
                                        JobConf conf,
                                        Reporter reporter) throws IOException {
        throw new UnsupportedOperationException("PxfInputFormat should not be used for reading data, but only for obtaining the splits of a file");
    }

    /*
     * Return true if this file can be split.
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return HdfsUtilities.isSplittableCodec(filename);
    }

}
