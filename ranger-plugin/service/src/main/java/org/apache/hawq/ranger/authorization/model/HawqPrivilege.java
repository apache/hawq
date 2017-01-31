/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hawq.ranger.authorization.model;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

/**
 *  Model enumeration of types of HAWQ privileges.
 */
public enum HawqPrivilege {
    select,
    insert,
    update,
    delete,
    references,
    usage,
    create,
    connect,
    execute,
    temp,
    create_schema,
    usage_schema;

    /**
     * Returns HawqPrivilege type by case-insensitive lookup of the value.
     * @param key case insensitive string representation of the privilege
     * @return instance of HawqPrivilege
     */
    @JsonCreator
    public static HawqPrivilege fromString(String key) {
        return key == null ? null : HawqPrivilege.valueOf(key.replace('-', '_').toLowerCase());
    }

    /**
     * Returns string representation of the enum, replaces underscores with dashes.
     * @return string representation of the enum
     */
    @JsonValue
    public String toValue() {
        return name().replace('_', '-');
    }
}
