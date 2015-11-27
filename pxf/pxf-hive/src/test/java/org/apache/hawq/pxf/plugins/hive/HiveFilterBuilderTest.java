package org.apache.hawq.pxf.plugins.hive;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
