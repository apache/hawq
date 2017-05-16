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

package org.apache.hawq.pxf.api.utilities;

public enum EnumAggregationType {

    COUNT("count", true);

    private String aggOperationCode;
    private boolean optimizationSupported;

    private EnumAggregationType(String aggOperationCode, boolean optimizationSupported) {
        this.aggOperationCode = aggOperationCode;
        this.optimizationSupported = optimizationSupported;
    }

    public String getAggOperationCode() {
        return this.aggOperationCode;
    }

    public boolean isOptimizationSupported() {
        return this.optimizationSupported;
    }

    public static EnumAggregationType getAggregationType(String aggOperationCode) {
        for (EnumAggregationType at : values()) {
            if (at.getAggOperationCode().equals(aggOperationCode)) {
                return at;
            }
        }
        return null;
    }
}
