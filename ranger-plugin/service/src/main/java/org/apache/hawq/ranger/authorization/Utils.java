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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class for reading values from the property file.
 */
public abstract class Utils {

    public static final String HAWQ = "hawq";
    public static final String UNKNOWN = "unknown";
    public static final String APP_ID_PROPERTY = "ranger.hawq.instance";
    public static final String VERSION_PROPERTY = "version";
    public static final String RANGER_SERVICE_PROPERTY_FILE = "rps.properties";

    private static final Log LOG = LogFactory.getLog(Utils.class);
    private static final Properties properties = readPropertiesFromFile();

    /**
     * Retrieves the app id from the environment variable with the key ranger.hawq.instance
     * or from the rps.properties file with the key ranger.hawq.instance
     *
     * If none exist, hawq is used as the default
     *
     * @return String id of the app
     */
    public static String getAppId() {
        return System.getProperty(APP_ID_PROPERTY, properties.getProperty(APP_ID_PROPERTY, HAWQ));
    }

    /**
     * Retrieves the version read from the property file.
     *
     * If none exist, unknown is used as the default
     *
     * @return version of the service
     */
    public static String getVersion() {
        return properties.getProperty(VERSION_PROPERTY, UNKNOWN);
    }

    /**
     * Reads properties from the property file.
     * @return properties read from the file
     */
    private static Properties readPropertiesFromFile() {
        Properties prop = new Properties();
        InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(RANGER_SERVICE_PROPERTY_FILE);
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            LOG.error("Failed to read from: " + RANGER_SERVICE_PROPERTY_FILE);
        }
        return prop;
    }
}
