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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;


public class RESTClient {

    private static final Log LOG = LogFactory.getLog(RESTClient.class);
    private static final String AUTH_HEADER = getAuthorizationHeader();

    private CloseableHttpClient httpClient;

    private static String getAuthorizationHeader() {
        return "Basic " + Base64.encodeBase64String("admin:admin".getBytes());
    }

    public RESTClient() {
        httpClient = HttpClients.createDefault();
    }

    public String executeRequest(Method method, String url) throws IOException {
        return executeRequest(method, url, null);
    }

    public String executeRequest(Method method, String url, String payload) throws IOException {
        HttpUriRequest request = null;
        switch (method) {
            case GET:
                request = new HttpGet(url);
                break;
            case POST:
                request = new HttpPost(url);
                ((HttpPost) request).setEntity(new StringEntity(payload));
                break;
            case DELETE:
                request = new HttpDelete(url);
                break;
            default:
                throw new IllegalArgumentException("Method " + method + " is not supported");
        }
        return executeRequest(request);
    }

    private String executeRequest(HttpUriRequest request) throws IOException {

        LOG.debug("--> request URI = " + request.getURI());

        request.setHeader("Authorization", AUTH_HEADER);
        request.setHeader("Content-Type", ContentType.APPLICATION_JSON.toString());

        CloseableHttpResponse response = httpClient.execute(request);
        String payload = null;
        try {
            int responseCode = response.getStatusLine().getStatusCode();
            LOG.debug("<-- response code = " + responseCode);

            HttpEntity entity = response.getEntity();
            if (entity != null) {
                payload = EntityUtils.toString(response.getEntity());
            }
            LOG.debug("<-- response payload = " + payload);

            if (responseCode == HttpStatus.SC_NOT_FOUND) {
                throw new ResourceNotFoundException();
            } else if (responseCode >= 300) {
                throw new ClientProtocolException("Unexpected HTTP response code = " + responseCode);
            }
        } finally {
            response.close();
        }

        return payload;
    }

    public static class ResourceNotFoundException extends IOException {

    }

    public enum Method {
        GET, POST, PUT, DELETE;
    }
}
