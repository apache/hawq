package org.apache.hawq.pxf.plugins.hbase;

import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseFilterBuilderTest {

    @Test
    public void parseNOTExpressionIgnored() throws Exception {
        String filter = "a1c2o1a1c2o2l0l2";
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(filter));
    }

}