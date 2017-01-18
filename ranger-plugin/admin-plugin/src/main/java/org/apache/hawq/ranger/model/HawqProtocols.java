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

package org.apache.hawq.ranger.model;

import java.util.ArrayList;
import java.util.List;

public enum HawqProtocols {

    PROTOCOL_FILE("file"),
    PROTOCOL_FTP("ftp"),
    PROTOCOL_HTTP("http"),
    PROTOCOL_GPFDIST("gpfdist"),
    PROTOCOL_GPFDISTS("gpfdists"),
    PROTOCOL_PXF("pxf");

    private final String name;

    private HawqProtocols(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static List<String> getAllProtocols() {
        List<String> protocols = new ArrayList<>();
        for(HawqProtocols protocol : HawqProtocols.values()) {
            protocols.add(protocol.getName());
        }
        return protocols;
    }
}
