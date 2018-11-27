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

import java.util.Set;

/**
 *  Model for response containing authorization decisions for access to multiple resources.
 */
public class AuthorizationResponse {

    private Integer requestId;
    private Set<ResourceAccess> access;

    /**
     * Returns request id.
     * @return id of the request
     */
    public Integer getRequestId() {
        return requestId;
    }

    /**
     * Sets request id.
     * @param requestId id of the request
     */
    public void setRequestId(Integer requestId) {
        this.requestId = requestId;
    }

    /**
     * Returns a set of <code>ResourceAccess</code> objects.
     * @return a set of <code>ResourceAccess</code> objects
     */
    public Set<ResourceAccess> getAccess() {
        return access;
    }

    /**
     * Sets <code>ResourceAccess</code> objects
     * @param access a set of <code>ResourceAccess</code> objects
     */
    public void setAccess(Set<ResourceAccess> access) {
        this.access = access;
    }
}
