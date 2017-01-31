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

package org.apache.hawq.pxf.api;

import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.utilities.EnumHawqType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class MetadataTest {

    @Test
    public void createFieldEmptyNameType() {
        try {
            Metadata.Field field = new Metadata.Field(null, null, false, null, null);
            fail("Empty name, type and source type shouldn't be allowed.");
        } catch (IllegalArgumentException e) {
            assertEquals("Field name, type and source type cannot be empty", e.getMessage());
        }
    }

    @Test
    public void createFieldNullType() {
        try {
            Metadata.Field field = new Metadata.Field("col1", null, "string");
            fail("Empty name, type and source type shouldn't be allowed.");
        } catch (IllegalArgumentException e) {
            assertEquals("Field name, type and source type cannot be empty", e.getMessage());
        }
    }
    @Test
    public void createItemEmptyNameType() {
        try {
            Metadata.Item item = new Metadata.Item(null, null);
            fail("Empty item name and path shouldn't be allowed.");
        } catch (IllegalArgumentException e) {
            assertEquals("Item or path name cannot be empty", e.getMessage());
        }
    }
}
