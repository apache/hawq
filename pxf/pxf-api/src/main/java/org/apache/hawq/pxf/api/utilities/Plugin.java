package org.apache.hawq.pxf.api.utilities;

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
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, Analyzer, ...).
 * Manages the meta data.
 */
public class Plugin {
    protected InputData inputData;

    /**
     * Constructs a plugin.
     *
     * @param input the input data
     */
    public Plugin(InputData input) {
        this.inputData = input;
    }

    /**
     * Checks if the plugin is thread safe or not, based on inputData.
     *
     * @return true if plugin is thread safe
     */
    public boolean isThreadSafe() {
        return true;
    }
}
