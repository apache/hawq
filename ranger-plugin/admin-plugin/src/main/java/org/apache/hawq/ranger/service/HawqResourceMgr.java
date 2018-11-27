/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hawq.ranger.service;

import org.apache.hawq.ranger.model.HawqResource;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Collections;

public abstract class HawqResourceMgr {

    public static List<String> getHawqResources(Map<String, String> configs,
                                                ResourceLookupContext context) throws SQLException {
        String userInput = context.getUserInput();
        HawqResource hawqResource = HawqResource.valueOf(context.getResourceName().toUpperCase());
        Map<String, List<String>> resources = context.getResources();

        List<String> result;
        HawqClient hawqClient = new HawqClient(configs);

        switch (hawqResource) {
            case DATABASE:
                result = hawqClient.getDatabaseList(userInput);
                break;
            case TABLESPACE:
                result = hawqClient.getTablespaceList(userInput);
                break;
            case PROTOCOL:
                result = hawqClient.getProtocolList(userInput);
                break;
            case SCHEMA:
                result = hawqClient.getSchemaList(userInput, resources);
                break;
            case LANGUAGE:
                result = hawqClient.getLanguageList(userInput, resources);
                break;
            case TABLE:
                result = hawqClient.getTableList(userInput, resources);
                break;
            case SEQUENCE:
                result = hawqClient.getSequenceList(userInput, resources);
                break;
            case FUNCTION:
                result = hawqClient.getFunctionList(userInput, resources);
                break;
            default:
                throw new IllegalArgumentException("Resource requested does not exist.");
        }

        Collections.sort(result);
        return result;
    }
}
