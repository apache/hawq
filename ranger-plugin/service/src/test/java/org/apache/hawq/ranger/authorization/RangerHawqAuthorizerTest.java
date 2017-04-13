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

package org.apache.hawq.ranger.authorization;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hawq.ranger.authorization.model.AuthorizationRequest;
import org.apache.hawq.ranger.authorization.model.AuthorizationResponse;
import org.apache.hawq.ranger.authorization.model.HawqPrivilege;
import org.apache.hawq.ranger.authorization.model.HawqResource;
import org.apache.hawq.ranger.authorization.model.ResourceAccess;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(UserGroupInformation.class)
public class RangerHawqAuthorizerTest {

    private static final Integer TEST_REQUEST_ID = 1;
    private static final String TEST_USER = "alex";
    private static final String TEST_CLIENT = "1.2.3.4";
    private static final String TEST_CONTEXT = "SELECT * FROM sales";
    private static final Set<HawqPrivilege> TEST_PRIVILEGES = Sets.newSet(HawqPrivilege.select, HawqPrivilege.update);

    private static final String TEST_RESOURCE_REQUEST =
            "finance:us:sales>select,update#finance:emea:sales>create";
    private static final String TEST_RESOURCE_RESPONSE_ALL_FALSE =
            "finance:us:sales>select,update>false#finance:emea:sales>create>false";
    private static final String TEST_RESOURCE_RESPONSE_ALL_TRUE =
            "finance:us:sales>select,update>true#finance:emea:sales>create>true";
    private static final String TEST_RESOURCE_RESPONSE_US_ALLOWED_EMEA_DENIED =
            "finance:us:sales>select,update>true#finance:emea:sales>create>false";
    private static final String TEST_RESOURCE_RESPONSE_UPDATE_DENIED =
            "finance:us:sales>select,update>false#finance:emea:sales>create>true";

    private static final String TEST_RESOURCE_REQUEST_CREATE_SCHEMA  = "finance>create";
    private static final String TEST_RESOURCE_RESPONSE_CREATE_SCHEMA = "finance>create>true";
    private static final String TEST_RESOURCE_REQUEST_USAGE_SCHEMA  = "finance:us>usage";
    private static final String TEST_RESOURCE_RESPONSE_USAGE_SCHEMA = "finance:us>usage>true";

    private RangerHawqAuthorizer authorizer;

    @Mock
    private RangerBasePlugin mockRangerPlugin;
    @Mock
    private RangerAccessResult mockRangerAccessResult;

    @Before
    public void setup() throws Exception {
        authorizer = RangerHawqAuthorizer.getInstance();
        authorizer.setRangerPlugin(mockRangerPlugin);
    }

    @Test
    public void testAuthorize_allAllowed() throws Exception {
        when(mockRangerPlugin.isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(mockRangerAccessResult);
        when(mockRangerAccessResult.getIsAllowed()).thenReturn(true);
        testRequest(TEST_RESOURCE_REQUEST, TEST_RESOURCE_RESPONSE_ALL_TRUE);
    }

    @Test
    public void testAuthorize_allDenied() throws Exception {
        when(mockRangerPlugin.isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(mockRangerAccessResult);
        when(mockRangerAccessResult.getIsAllowed()).thenReturn(false);
        testRequest(TEST_RESOURCE_REQUEST, TEST_RESOURCE_RESPONSE_ALL_FALSE);
    }

    @Test
    public void testAuthorize_usAllowedEmeaDenied() throws Exception {
        RangerAccessResult mockRangerAccessResultUS = mock(RangerAccessResult.class);
        RangerAccessResult mockRangerAccessResultEMEA = mock(RangerAccessResult.class);

        when(mockRangerPlugin.isAccessAllowed(argThat(new SchemaMatcher("us")))).thenReturn(mockRangerAccessResultUS);
        when(mockRangerPlugin.isAccessAllowed(argThat(new SchemaMatcher("emea")))).thenReturn(mockRangerAccessResultEMEA);
        when(mockRangerAccessResultUS.getIsAllowed()).thenReturn(true);
        when(mockRangerAccessResultEMEA.getIsAllowed()).thenReturn(false);
        testRequest(TEST_RESOURCE_REQUEST, TEST_RESOURCE_RESPONSE_US_ALLOWED_EMEA_DENIED);
    }

    @Test
    public void testAuthorize_partialPrivilegeUpdateDenied() throws Exception {
        RangerAccessResult mockRangerAccessResultCreateSelect = mock(RangerAccessResult.class);
        RangerAccessResult mockRangerAccessResultUpdate = mock(RangerAccessResult.class);

        when(mockRangerPlugin.isAccessAllowed(argThat(new PrivilegeMatcher("create", "select")))).thenReturn(mockRangerAccessResultCreateSelect);
        when(mockRangerPlugin.isAccessAllowed(argThat(new PrivilegeMatcher("update")))).thenReturn(mockRangerAccessResultUpdate);
        when(mockRangerAccessResultCreateSelect.getIsAllowed()).thenReturn(true);
        when(mockRangerAccessResultUpdate.getIsAllowed()).thenReturn(false);
        testRequest(TEST_RESOURCE_REQUEST, TEST_RESOURCE_RESPONSE_UPDATE_DENIED);
    }

    @Test
    public void testAuthorize_createSchemaAllowed() throws Exception {
        RangerAccessResult mockRangerAccessResultCreate = mock(RangerAccessResult.class);

        when(mockRangerPlugin.isAccessAllowed(argThat(new PrivilegeMatcher("create-schema")))).thenReturn(mockRangerAccessResultCreate);
        when(mockRangerAccessResultCreate.getIsAllowed()).thenReturn(true);
        testRequest(TEST_RESOURCE_REQUEST_CREATE_SCHEMA, TEST_RESOURCE_RESPONSE_CREATE_SCHEMA);
    }

    @Test
    public void testAuthorize_usageSchemaAllowed() throws Exception {
        RangerAccessResult mockRangerAccessResultUsage = mock(RangerAccessResult.class);

        when(mockRangerPlugin.isAccessAllowed(argThat(new PrivilegeMatcher("usage-schema")))).thenReturn(mockRangerAccessResultUsage);
        when(mockRangerAccessResultUsage.getIsAllowed()).thenReturn(true);
        testRequest(TEST_RESOURCE_REQUEST_USAGE_SCHEMA, TEST_RESOURCE_RESPONSE_USAGE_SCHEMA);
    }

    @Test
    public void testAuthorize_allAllowed_group() throws Exception {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getGroupNames()).thenReturn(new String[]{"foo", "bar"});
        PowerMockito.mockStatic(UserGroupInformation.class);
        when(UserGroupInformation.createRemoteUser(TEST_USER)).thenReturn(mockUgi);
        when(mockRangerPlugin.isAccessAllowed(argThat(new UGIMatcher(TEST_USER, "foo", "bar")))).thenReturn(mockRangerAccessResult);
        when(mockRangerAccessResult.getIsAllowed()).thenReturn(true);
        testRequest(TEST_RESOURCE_REQUEST, TEST_RESOURCE_RESPONSE_ALL_TRUE);
    }

    @Test
    public void testAuthorize_allAllowed_noGroup() throws Exception {
        when(mockRangerPlugin.isAccessAllowed(argThat(new UGIMatcher(TEST_USER, null)))).thenReturn(mockRangerAccessResult);
        when(mockRangerAccessResult.getIsAllowed()).thenReturn(true);
        testRequest(TEST_RESOURCE_REQUEST, TEST_RESOURCE_RESPONSE_ALL_TRUE);
    }

    /* ----- VALIDATION TESTS ----- */

    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_requestId() {
        AuthorizationRequest request = prepareRequest(null, TEST_USER, TEST_CLIENT, TEST_CONTEXT, TEST_RESOURCE_REQUEST);
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_user() {
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, "", TEST_CLIENT, TEST_CONTEXT, TEST_RESOURCE_REQUEST);
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_client() {
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, TEST_USER, "", TEST_CONTEXT, TEST_RESOURCE_REQUEST);
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_context() {
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, TEST_USER, TEST_CLIENT, "", TEST_RESOURCE_REQUEST);
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_emptyAccessSet() {
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, TEST_USER, TEST_CLIENT, TEST_CONTEXT, new HashSet<ResourceAccess>());
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_emptyResource() {
        ResourceAccess resourceAccess = new ResourceAccess();
        resourceAccess.setResource(new HashMap<HawqResource, String>());
        resourceAccess.setPrivileges(TEST_PRIVILEGES);
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, TEST_USER, TEST_CLIENT, TEST_CONTEXT, resourceAccess);
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_emptyResourceValue() {
        ResourceAccess resourceAccess = new ResourceAccess();
        HashMap<HawqResource, String> resource = new HashMap<>();
        resource.put(HawqResource.database, "");
        resourceAccess.setResource(resource);
        resourceAccess.setPrivileges(TEST_PRIVILEGES);
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, TEST_USER, TEST_CLIENT, TEST_CONTEXT, resourceAccess);
        authorizer.isAccessAllowed(request);
    }
    @Test(expected=IllegalArgumentException.class)
    public void testAuthorize_validationFailure_emptyPrivileges() {
        ResourceAccess resourceAccess = new ResourceAccess();
        HashMap<HawqResource, String> resource = new HashMap<>();
        resource.put(HawqResource.database, "abc");
        resourceAccess.setResource(resource);
        resourceAccess.setPrivileges(new HashSet<HawqPrivilege>());
        AuthorizationRequest request = prepareRequest(TEST_REQUEST_ID, TEST_USER, TEST_CLIENT, TEST_CONTEXT, resourceAccess);
        authorizer.isAccessAllowed(request);
    }

    /* ----- HELPER METHODS ----- */

    private void testRequest(String request, String expectedResponse) {
        AuthorizationRequest authRequest = prepareRequest(TEST_REQUEST_ID, TEST_USER, TEST_CLIENT, TEST_CONTEXT, request);
        AuthorizationResponse authResponse = authorizer.isAccessAllowed(authRequest);
        validateResponse(authResponse, expectedResponse);
    }

    private AuthorizationRequest prepareRequest(
            Integer requestId, String user, String clientIp, String context, Set<ResourceAccess> access) {

        AuthorizationRequest request = new AuthorizationRequest();
        request.setRequestId(requestId);
        request.setUser(user);
        request.setClientIp(clientIp);
        request.setContext(context);
        request.setAccess(access);

        return request;
    }

    private AuthorizationRequest prepareRequest(
            Integer requestId, String user, String clientIp, String context, ResourceAccess resourceAccess) {

        Set<ResourceAccess> access = new HashSet<>();
        access.add(resourceAccess);
        return prepareRequest(requestId, user, clientIp, context, access);
    }

    private AuthorizationRequest prepareRequest(
            Integer requestId, String user, String clientIp, String context, String resources) {

        Set<ResourceAccess> access = new HashSet<>();
        // resource string is like "db:schema:table>select,update#db:schema:table>create"
        for (String resourceStr : resources.split("#")) {
            String[] parts = resourceStr.split(">");
            String[] resource = parts[0].split(":");
            String[] privs = parts[1].split(",");

            Map<HawqResource, String> tableResource = new HashMap<>();
            tableResource.put(HawqResource.database, resource[0]);
            if (resource.length > 1) {
                tableResource.put(HawqResource.schema, resource[1]);
            }
            if (resource.length > 2) {
                tableResource.put(HawqResource.table, resource[2]);
            }
            ResourceAccess tableAccess = new ResourceAccess();
            tableAccess.setResource(tableResource);

            Set<HawqPrivilege> privSet = new HashSet<>();
            for (String priv : privs) {
                privSet.add(HawqPrivilege.valueOf(priv));
            }
            tableAccess.setPrivileges(privSet);
            access.add(tableAccess);
        }

        return prepareRequest(requestId, user, clientIp, context, access);
    }

    private void validateResponse(AuthorizationResponse response, String resources) {

        assertNotNull(response);

        Set<ResourceAccess> actual = response.getAccess();
        Set<ResourceAccess> expected = new HashSet<>();

        // resources string is like "db:schema:table>select,update>true#db:schema:table>create>false"
        for (String resourceStr : resources.split("#")) {
            String[] parts = resourceStr.split(">");
            String[] resource = parts[0].split(":");
            String[] privs = parts[1].split(",");
            Boolean allowed = Boolean.valueOf(parts[2]);

            Map<HawqResource, String> tableResource = new HashMap<>();
            tableResource.put(HawqResource.database, resource[0]);
            if (resource.length > 1) {
                tableResource.put(HawqResource.schema, resource[1]);
            }
            if (resource.length > 2) {
                tableResource.put(HawqResource.table, resource[2]);
            }
            ResourceAccess tableAccess = new ResourceAccess();
            tableAccess.setResource(tableResource);

            Set<HawqPrivilege> privSet = new HashSet<>();
            for (String priv : privs) {
                privSet.add(HawqPrivilege.fromString(priv));
            }
            tableAccess.setPrivileges(privSet);
            tableAccess.setAllowed(allowed);

            expected.add(tableAccess);
        }

        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    /* ----- Argument Matchers ----- */

    private class SchemaMatcher extends ArgumentMatcher<RangerAccessRequest> {
        private String schema;
        public SchemaMatcher(String schema) {
            this.schema = schema;
        }
        @Override
        public boolean matches(Object request) {
            return request == null ? false :
                    schema.equals(((RangerAccessRequest) request).getResource().getAsMap().get("schema"));
        }
    };

    private class PrivilegeMatcher extends ArgumentMatcher<RangerAccessRequest> {
        private Set<String> privileges;
        public PrivilegeMatcher(String... privileges) {
            this.privileges = Sets.newSet(privileges);
        }
        @Override
        public boolean matches(Object request) {
            return request == null ? false :
                    privileges.contains(((RangerAccessRequest) request).getAccessType());
        }
    };

    private class UGIMatcher extends ArgumentMatcher<RangerAccessRequest> {
        private String user;
        private Set<String> groups;
        public UGIMatcher(String user, String... groups) {
            this.user = user;
            this.groups = groups == null ? Collections.<String>emptySet() : Sets.newSet(groups);
        }
        @Override
        public boolean matches(Object request) {
            return request == null ? false :
                    user.equals(((RangerAccessRequest) request).getUser()) &&
                    CollectionUtils.isEqualCollection(groups, (((RangerAccessRequest) request).getUserGroups()));
        }
    };

}
