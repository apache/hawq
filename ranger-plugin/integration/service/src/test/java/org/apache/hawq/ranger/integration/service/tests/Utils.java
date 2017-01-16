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

package org.apache.hawq.ranger.integration.service.tests;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class Utils {

    protected static final Log log = LogFactory.getLog(Utils.class);

    public static String getPayload(String jsonFile)
            throws IOException {
        return IOUtils.toString(Utils.class.getClassLoader().getResourceAsStream(jsonFile));
    }

    public static String getEncoding() {
        return Base64.encodeBase64String("admin:admin".getBytes());
    }

    public static String processHttpRequest(HttpRequestBase request)
            throws IOException {

        if (log.isDebugEnabled()) {
            log.debug("Request URI = " + request.getURI().toString());
        }
        request.setHeader("Authorization", "Basic " + getEncoding());
        request.setHeader("Content-Type", "application/json");
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpResponse response = httpClient.execute(request);
        int responseCode = response.getStatusLine().getStatusCode();
        log.info("Response Code = " + responseCode);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            String result = IOUtils.toString(entity.getContent());
            if (log.isDebugEnabled()) {
                log.debug(result);
            }
            return result;
        }
        return null;
    }

    public static RPSResponse getResponse(String result)
            throws IOException {
        return new ObjectMapper().readValue(result, RPSResponse.class);
    }

}
