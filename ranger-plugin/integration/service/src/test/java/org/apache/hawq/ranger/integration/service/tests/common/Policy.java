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

package org.apache.hawq.ranger.integration.service.tests.common;

import java.util.*;

public class Policy {

    public enum ResourceType {
        database, schema, table, function, sequence, tablespace, language, protocol;
    }

    public static class ResourceValue {
        public Set<String> values = new HashSet<>();
        public Boolean isExcludes = false;
        public Boolean isRecursive = false;

        public ResourceValue(String... values) {
            this.values.addAll(Arrays.asList(values));
        }
    }

    public static class Access {
        public String type;
        public Boolean isAllowed = true;
        public Access(String type) {
            this.type = type;
        }
    }

    public static class PolicyItem {
        public Set<Access> accesses = new HashSet<>();
        public Set<String> users = new HashSet<>();
        public Set<String> groups = new HashSet<>();
        public Set<String> conditions = new HashSet<>();
        public Boolean delegateAdmin = true;
        public PolicyItem(String[] privileges) {
            for (String privilege : privileges) {
                this.accesses.add(new Access(privilege));
            }
        }
    }

    public Boolean isEnabled = true;
    public String service = "hawq";
    public String name;
    public Integer policyType = 0;
    public String description = "Test policy";
    public Boolean isAuditEnabled = true;
    public Map<ResourceType, ResourceValue> resources = new HashMap<>();
    public Set<PolicyItem> policyItems = new HashSet<>();
    public Set<Object> denyPolicyItems = new HashSet<>();
    public Set<Object> allowExceptions = new HashSet<>();
    public Set<Object> denyExceptions = new HashSet<>();
    public Set<Object> dataMaskPolicyItems = new HashSet<>();
    public Set<Object> rowFilterPolicyItems = new HashSet<>();

    // do not serialize into JSON
    public transient boolean isParentStar = false;
    public transient boolean isChildStar = false;

    public static class PolicyBuilder {
        private Policy policy = new Policy();

        public PolicyBuilder name(String name) {
            policy.name = name;
            policy.description = "Test Policy for " + name;
            return this;
        }
        public PolicyBuilder resource(ResourceType type, String value) {
            policy.resources.put(type, new ResourceValue(value));
            return this;
        }
        public PolicyBuilder userAccess(String user, String... privileges) {
            PolicyItem policyItem = new PolicyItem(privileges);
            policyItem.users.add(user);
            policy.policyItems.add(policyItem);
            return this;
        }
        public PolicyBuilder groupAccess(String group, String... privileges) {
            PolicyItem policyItem = new PolicyItem(privileges);
            policyItem.groups.add(group);
            policy.policyItems.add(policyItem);
            return this;
        }
        public Policy build() {
            return policy;
        }
    }
}