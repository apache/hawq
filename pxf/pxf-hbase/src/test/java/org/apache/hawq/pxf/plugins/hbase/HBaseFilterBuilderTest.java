package org.apache.hawq.pxf.plugins.hbase;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class HBaseFilterBuilderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void parseNOTExpressionIgnored() throws Exception {
        String filter = "a1c2o1a1c2o2l0l2";
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(filter));

        filter = "a1c2o1a1c2o2l2l0";
        builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(filter));
    }

    @Test
    public void parseNOTOpCodeInConstant() throws Exception {
        String filter = "a1c\"l2\"o1a1c2o2l0";
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        //Testing that we get past the parsing stage
        //Very crude but it avoids instantiating all the necessary dependencies
        thrown.expect(NullPointerException.class);
        builder.getFilterObject(filter);
    }

}