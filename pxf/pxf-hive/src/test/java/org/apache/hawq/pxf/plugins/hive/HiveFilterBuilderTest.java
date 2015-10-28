package org.apache.hawq.pxf.plugins.hive;

import org.junit.Test;

import java.util.List;

import static org.apache.hawq.pxf.api.FilterParser.BasicFilter;
import static org.apache.hawq.pxf.api.FilterParser.Operation;
import static org.apache.hawq.pxf.api.FilterParser.Operation.*;
import static org.junit.Assert.assertEquals;

public class HiveFilterBuilderTest {
    @Test
    public void parseFilterWithThreeOperations() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        String[] consts = new String[] {"first", "2", "3"};
        Operation[] ops = new Operation[] {HDOP_EQ, HDOP_GT, HDOP_LT};
        int[] idx = new int[] {1, 2, 3};

        @SuppressWarnings("unchecked")
        List<BasicFilter> filterList = (List) builder.getFilterObject("a1c\"first\"o5a2c2o2o7a3c3o1o7");
        assertEquals(consts.length, filterList.size());
        for (int i = 0; i < filterList.size(); i++) {
            BasicFilter filter = filterList.get(i);
            assertEquals(filter.getConstant().constant().toString(), consts[i]);
            assertEquals(filter.getOperation(), ops[i]);
            assertEquals(filter.getColumn().index(), idx[i]);
        }
    }
}