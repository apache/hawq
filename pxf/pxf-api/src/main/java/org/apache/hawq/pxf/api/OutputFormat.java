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
 * PXF supported output formats: {@link org.apache.hawq.pxf.service.io.Text} and {@link org.apache.hawq.pxf.service.io.GPDBWritable}
 */
public enum OutputFormat {
    TEXT("org.apache.hawq.pxf.service.io.Text"),
    GPDBWritable("org.apache.hawq.pxf.service.io.GPDBWritable");

    private String className;

    OutputFormat(String className) {
        this.className = className;
    }

    /**
     * Returns a formats's implementation class name
     *
     * @param fields the item's output parameters
     */
    public String getClassName() {
        return className;
    }

    public static OutputFormat getOutputFormat(String className) {
        for (OutputFormat of : values()) {
            if (of.getClassName().equals(className)) {
                return of;
            }
        }
        throw new UnsupportedTypeException("Unable to find output format by given class name: " + className);
    }
}
