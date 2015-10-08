package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.utilities.InputData;

import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

import static com.pivotal.pxf.plugins.hive.HiveInputFormatFragmenter.PXF_HIVE_SERDES;

/**
 * Specialization of HiveAccessor for a Hive table that stores only RC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as RC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveRCFileAccessor extends HiveAccessor {

    /**
     * Constructs a HiveRCFileAccessor.
     *
     * @param input input containing user data
     * @throws Exception if user data was wrong
     */
    public HiveRCFileAccessor(InputData input) throws Exception {
        super(input, new RCFileInputFormat());
        String[] toks = HiveInputFormatFragmenter.parseToks(input, PXF_HIVE_SERDES.COLUMNAR_SERDE.name(), PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE.name());
        initPartitionFields(toks[HiveInputFormatFragmenter.TOK_KEYS]);
        filterInFragmenter = new Boolean(toks[HiveInputFormatFragmenter.TOK_FILTER_DONE]);
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new RCFileRecordReader(jobConf, (FileSplit) split);
    }
}
