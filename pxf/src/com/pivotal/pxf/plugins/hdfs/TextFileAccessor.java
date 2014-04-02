package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.utilities.InputData;

/*
 * @deprecated - use LineBreakAccessor
 */
@Deprecated
public class TextFileAccessor extends LineBreakAccessor {
    /*
     * C'tor
     * Creates the TextFileAccessor
     */
    public TextFileAccessor(InputData input) throws Exception {
        super(input);
    }
}
