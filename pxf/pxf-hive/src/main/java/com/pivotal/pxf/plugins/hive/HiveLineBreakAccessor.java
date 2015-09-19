package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.utilities.InputData;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

import static com.pivotal.pxf.plugins.hive.HiveInputFormatFragmenter.PXF_HIVE_SERDES;

/**
 * Specialization of HiveAccessor for a Hive table stored as Text files.
 * Use together with HiveInputFormatFragmenter/HiveStringPassResolver
 */
public class HiveLineBreakAccessor extends HiveAccessor {

    /**
     * Constructs a HiveLineBreakAccessor.
     */
    public HiveLineBreakAccessor(InputData input) throws Exception {
        super(input, new TextInputFormat());
        ((TextInputFormat) inputFormat).configure(jobConf);
        String[] toks = HiveInputFormatFragmenter.parseToks(input, PXF_HIVE_SERDES.LAZY_SIMPLE_SERDE.name());
        initPartitionFields(toks[HiveInputFormatFragmenter.TOK_KEYS]);
        filterInFragmenter = new Boolean(toks[HiveInputFormatFragmenter.TOK_FILTER_DONE]);
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new LineRecordReader(jobConf, (FileSplit) split);
    }
}
