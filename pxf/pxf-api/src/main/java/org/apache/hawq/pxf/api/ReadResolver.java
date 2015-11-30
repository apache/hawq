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


import java.util.List;

/**
 * Interface that defines the deserialization of one record brought from the {@link ReadAccessor}.
 * All deserialization methods (e.g, Writable, Avro, ...) implement this interface.
 */
public interface ReadResolver {
    /**
     * Gets the {@link OneField} list of one row.
     *
     * @param row the row to get the fields from
     * @return the {@link OneField} list of one row.
     * @throws Exception if decomposing the row into fields failed
     */
    List<OneField> getFields(OneRow row) throws Exception;
}
