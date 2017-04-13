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

package org.apache.hawq.ranger.authorization;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hawq.ranger.authorization.model.AuthorizationRequest;
import org.apache.hawq.ranger.authorization.model.AuthorizationResponse;
import org.apache.hawq.ranger.authorization.model.HawqPrivilege;
import org.apache.hawq.ranger.authorization.model.HawqResource;
import org.apache.hawq.ranger.authorization.model.ResourceAccess;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.*;

import static org.apache.hawq.ranger.authorization.Utils.APPID;
import static org.apache.hawq.ranger.authorization.Utils.HAWQ;

/**
 * Authorizer implementation that uses Ranger to make access decision. Implemented as a singleton.
 */
public class RangerHawqAuthorizer implements HawqAuthorizer {

    private static final Log LOG = LogFactory.getLog(RangerHawqAuthorizer.class);

    private static final RangerHawqAuthorizer INSTANCE = new RangerHawqAuthorizer();

    private RangerBasePlugin rangerPlugin;

    /**
     * Returns the instance of the RangerHawqAuthorizer.
     * @return the singleton instance
     */
    public static RangerHawqAuthorizer getInstance() {
        return INSTANCE;
    }

    /**
     * Constructor. Initializes Ranger Base Plugin to fetch policies from Ranger.
     */
    private RangerHawqAuthorizer() {

        LOG.info("********** Initializing RangerHawqAuthorizer **********");

        String instance = Utils.getInstanceName();

        LOG.info(String.format("Initializing RangerBasePlugin for service %s:%s:%s", HAWQ, instance, APPID));
        rangerPlugin = new RangerBasePlugin(HAWQ, APPID);
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
        rangerPlugin.init();
        LOG.info(String.format("********** Initialized RangerBasePlugin for service %s:%s:%s **********", HAWQ, instance, APPID));
    }

    @Override
    public AuthorizationResponse isAccessAllowed(AuthorizationRequest request) {

        // validate request to make sure no data is missing
        validateRequest(request);

        // prepare response object
        AuthorizationResponse response = new AuthorizationResponse();
        response.setRequestId(request.getRequestId());
        Set<ResourceAccess> access = new HashSet<>();
        response.setAccess(access);

        // iterate over resource requests, augment processed ones with the decision and add to the response
        for (ResourceAccess resourceAccess : request.getAccess()) {
            boolean accessAllowed = authorizeResource(resourceAccess, request.getUser(), request.getClientIp(), request.getContext());
            resourceAccess.setAllowed(accessAllowed);
            access.add(resourceAccess);
        }

        return response;
    }

    /**
     * Authorizes access to a single resource for a given user.
     *
     * @param resourceAccess resource to authorize access to
     * @param user user requesting authorization
     * @return true if access is authorized, false otherwise
     */
    private boolean authorizeResource(ResourceAccess resourceAccess, String user, String clientIp, String context) {

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Request: access for user=%s to resource=%s with privileges=%s",
                user, resourceAccess.getResource(), resourceAccess.getPrivileges()));
        }

        RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
        //resource.setOwnerUser();
        for (Map.Entry<HawqResource, String> resourceEntry : resourceAccess.getResource().entrySet()) {
            rangerResource.setValue(resourceEntry.getKey().name(), resourceEntry.getValue());
        }
        // determine user groups
        Set<String> userGroups = getUserGroups(user);

        boolean accessAllowed = true;
        // iterate over all privileges requested
        for (HawqPrivilege privilege : resourceAccess.getPrivileges()) {
            boolean privilegeAuthorized = authorizeResourcePrivilege(rangerResource, privilege.name(), user, userGroups, clientIp, context);
            // ALL model of evaluation -- all privileges must be authorized for access to be allowed
            if (!privilegeAuthorized) {
                accessAllowed = false;
                break; // terminate early if even a single privilege is not authorized
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Decision: accessAllowed=%s for user=%s to resource=%s with privileges=%s",
                    accessAllowed, user, resourceAccess.getResource(), resourceAccess.getPrivileges()));
        }

        return accessAllowed;
    }

    /**
     * Authorizes access of a given type (privilege) to a single resource for a given user.
     *
     * @param rangerResource resource to authorize access to
     * @param accessType privilege requested for a given resource
     * @param user user requesting authorization
     * @param userGroups groups a user belongs to
     * @return true if access is authorized, false otherwise
     */
    private boolean authorizeResourcePrivilege(RangerAccessResource rangerResource, String accessType, String user, Set<String> userGroups, String clientIp, String context) {

        Map<String, String> resourceMap = rangerResource.getAsMap();
        String database = resourceMap.get(HawqResource.database.name());
        String schema = resourceMap.get(HawqResource.schema.name());
        int resourceSize = resourceMap.size();

        // special handling for non-leaf policies
        if (accessType.equals(HawqPrivilege.create.name()) && database != null && schema == null && resourceSize == 1) {
            accessType = HawqPrivilege.create_schema.toValue();
            LOG.debug("accessType mapped to: create-schema");
        } else if (accessType.equals(HawqPrivilege.usage.name()) && database != null && schema != null && resourceSize == 2) {
            accessType = HawqPrivilege.usage_schema.toValue();
            LOG.debug("accessType mapped to: usage-schema");
        }

        RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl(rangerResource, accessType, user, userGroups);
        rangerRequest.setAccessTime(new Date());
        rangerRequest.setAction(accessType);
        rangerRequest.setClientIPAddress(clientIp);
        rangerRequest.setRequestData(context);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(rangerRequest);
        boolean accessAllowed = result != null && result.getIsAllowed();

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("--- RangerDecision: accessAllowed=%s for user=%s to resource=%s with privileges=%s, result present=%s",
                    accessAllowed, user, rangerResource.getAsString(), accessType, result!=null));
        }

        return accessAllowed;
    }

    /**
     * Validates that authorization requests do not have any missing data.
     *
     * @param request authorization request
     * @throws IllegalArgumentException if any data is missing
     */
    private void validateRequest(AuthorizationRequest request) {
        LOG.debug("Validating authorization request");

        if (request == null) {
            throw new IllegalArgumentException("request is null");
        }

        if (request.getRequestId() == null) {
            throw new IllegalArgumentException("requestId field is missing or null in the request");
        }

        if (StringUtils.isEmpty(request.getUser())) {
            throw new IllegalArgumentException("user field is missing or empty in the request");
        }

        if (StringUtils.isEmpty(request.getClientIp())) {
            throw new IllegalArgumentException("clientIp field is missing or empty in the request");
        }

        if (StringUtils.isEmpty(request.getContext())) {
            throw new IllegalArgumentException("context field is missing or empty in the request");
        }

        Set<ResourceAccess> accessSet = request.getAccess();
        if (CollectionUtils.isEmpty(accessSet)) {
            throw new IllegalArgumentException("access field is missing or empty in the request");
        }

        for (ResourceAccess access : accessSet) {
            validateResourceAccess(access);
        }

        LOG.debug("Successfully validated authorization request");
    }

    /**
     * Validates that resource access does not have any missing data.
     *
     * @param access resource access data
     * @throws IllegalArgumentException if any data is missing
     */
    private void validateResourceAccess(ResourceAccess access) {
        Map<HawqResource, String> resourceMap = access.getResource();
        if  (MapUtils.isEmpty(resourceMap)) {
            throw new IllegalArgumentException("resource field is missing or empty in the request");
        }
        for (Map.Entry<HawqResource, String> resourceEntry : resourceMap.entrySet()) {
            if (StringUtils.isEmpty(resourceEntry.getValue())) {
                throw new IllegalArgumentException(
                        String.format("resource value is missing for key=%s in the request", resourceEntry.getKey())
                );
            }
        }
        if (CollectionUtils.isEmpty(access.getPrivileges())) {
            throw new IllegalArgumentException("set of privileges is missing empty in the request");
        }
    }

    /**
     * Returns a set of groups the user belongs to
     * @param user user name
     * @return set of groups for the user
     */
    private Set<String> getUserGroups(String user) {
        String[] userGroups = null;
        try {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
            userGroups = ugi.getGroupNames();
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Determined user=%s belongs to groups=%s", user, Arrays.toString(userGroups)));
            }
        } catch (Throwable e) {
            LOG.warn("Failed to determine groups for user=" + user, e);
        }
        return userGroups == null ? Collections.<String>emptySet() : new HashSet<String>(Arrays.asList(userGroups));
    }



    /**
     * Sets an instance of the Ranger Plugin for testing.
     *
     * @param plugin plugin instance to use while testing
     */
    void setRangerPlugin(RangerBasePlugin plugin) {
        rangerPlugin = plugin;
    }

    /**
     * Returns the instance of the Ranger Plugin for testing.
     *
     * @return BaseRangerPlugin instance
     */
    RangerBasePlugin getRangerPlugin() {
        return rangerPlugin;
    }
}
