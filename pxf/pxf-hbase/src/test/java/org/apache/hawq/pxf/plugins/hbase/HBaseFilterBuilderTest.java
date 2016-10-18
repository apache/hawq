package org.apache.hawq.pxf.plugins.hbase;

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