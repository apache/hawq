package org.apache.hawq.pxf.api;

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


/**
 * Defines a one field in a deserialized record.
 */
public class OneField {
    /** OID value recognized by GPDBWritable. */
    public int type;

    /** Field value. */
    public Object val;

    public OneField() {
    }

    /**
     * Constructs a OneField object.
     *
     * @param type the OID value recognized by GPDBWritable
     * @param val the field value
     */
    public OneField(int type, Object val) {
        this.type = type;
        this.val = val;
    }

    @Override
    public String toString() {
        return val.toString();
    }
}
