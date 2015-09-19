package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;

import java.util.Collections;
import java.util.List;

import static com.pivotal.pxf.api.io.DataType.VARCHAR;

/**
 * Specialized HiveResolver for a Hive table stored as Text files.
 * Use together with HiveInputFormatFragmenter/HiveLineBreakAccessor.
 */
public class HiveStringPassResolver extends HiveResolver {
    private StringBuilder parts;

    public HiveStringPassResolver(InputData input) throws Exception {
        super(input);
    }

    @Override
    void parseUserData(InputData input) throws Exception {
        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HiveDataFragmenter.HIVE_UD_DELIM);
        parseDelimiterChar(input);
        parts = new StringBuilder();
        partitionKeys = toks[HiveInputFormatFragmenter.TOK_KEYS];
    }

    @Override
    void initSerde(InputData input) {
        /* nothing to do here */
    }

    @Override
    void initPartitionFields() {
        initPartitionFields(parts);
    }

    /**
     * getFields returns a singleton list of OneField item.
     * OneField item contains two fields: an integer representing the VARCHAR type and a Java
     * Object representing the field value.
     */
    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        String line = (onerow.getData()).toString();

        /* We follow Hive convention. Partition fields are always added at the end of the record */
        return Collections.singletonList(new OneField(VARCHAR.getOID(), line + parts));
    }
}
