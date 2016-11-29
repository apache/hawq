package org.apache.hawq.pxf.service;

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class ProfileFactory {

    private static final String HIVE_TEXT_PROFILE = "HiveText";
    private static final String HIVE_ORC_PROFILE = "HiveOrc";

    public static String get(InputFormat inputFormat) throws Exception {
        String profileName = null;
        if (inputFormat instanceof TextInputFormat) {
            profileName = HIVE_TEXT_PROFILE;
        } else if (inputFormat instanceof OrcInputFormat) {
            profileName = HIVE_ORC_PROFILE;
        }

        return profileName;
    }

}
