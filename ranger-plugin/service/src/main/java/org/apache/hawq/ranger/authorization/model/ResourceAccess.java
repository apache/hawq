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

import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Model object for requesting access to a single resource.
 */
public class ResourceAccess {

    private Map<HawqResource, String> resource;
    private Set<HawqPrivilege> privileges;
    private boolean allowed = false;

    public Set<HawqPrivilege> getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Set<HawqPrivilege> privileges) {
        this.privileges = privileges;
    }

    public boolean isAllowed() {
        return allowed;
    }

    public void setAllowed(boolean allowed) {
        this.allowed = allowed;
    }

    public Map<HawqResource, String> getResource() {
        return resource;
    }

    public void setResource(Map<HawqResource, String> resource) {
        this.resource = resource;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("resource", resource)
                .append("privileges", privileges)
                .append("allowed", allowed)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceAccess that = (ResourceAccess) o;
        return allowed == that.allowed &&
                Objects.equals(resource, that.resource) &&
                Objects.equals(privileges, that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, privileges, allowed);
    }

}
