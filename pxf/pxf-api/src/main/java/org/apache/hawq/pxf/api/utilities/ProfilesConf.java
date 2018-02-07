package org.apache.hawq.pxf.api.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hawq.pxf.api.utilities.ProfileConfException.MessageFormat.*;

/**
 * This enum holds the profiles files: pxf-profiles.xml and pxf-profiles-default.xml.
 * It exposes a public static method getProfilePluginsMap(String plugin) which returns the requested profile plugins
 */
public enum ProfilesConf {
    INSTANCE; // enum singleton
    // not necessary to declare LOG as static final, because this is a singleton
    private Log LOG = LogFactory.getLog(ProfilesConf.class);
    private Map<String, Map<String, String>> profilesMap;
    private final static String EXTERNAL_PROFILES = "pxf-profiles.xml";
    private final static String INTERNAL_PROFILES = "pxf-profiles-default.xml";

    /**
     * Constructs the ProfilesConf enum singleton instance.
     * <p/>
     * External profiles take precedence over the internal ones and override them.
     */
    private ProfilesConf() {
        profilesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        loadConf(INTERNAL_PROFILES, true);
        loadConf(EXTERNAL_PROFILES, false);
        if (profilesMap.isEmpty()) {
            throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, EXTERNAL_PROFILES);
        }
        LOG.info("PXF profiles loaded: " + profilesMap.keySet());
    }

    /**
     * Get requested profile plugins map.
     * In case pxf-profiles.xml is not on the classpath, or it doesn't contains the requested profile,
     * Fallback to pxf-profiles-default.xml occurs (@see useProfilesDefaults(String msgFormat))
     *
     * @param profile The requested profile
     * @return Plugins map of the requested profile
     */
    public static Map<String, String> getProfilePluginsMap(String profile) {
        Map<String, String> pluginsMap = INSTANCE.profilesMap.get(profile);
        if (pluginsMap == null) {
            throw new ProfileConfException(NO_PROFILE_DEF, profile, EXTERNAL_PROFILES);
        }
        return pluginsMap;
    }

    private ClassLoader getClassLoader() {
        ClassLoader cL = Thread.currentThread().getContextClassLoader();
        return (cL != null)
                ? cL
                : ProfilesConf.class.getClassLoader();
    }

    private void loadConf(String fileName, boolean isMandatory) {
        URL url = getClassLoader().getResource(fileName);
        if (url == null) {
            LOG.warn(fileName + " not found in the classpath");
            if (isMandatory) {
                throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, fileName);
            }
            return;
        }
        try {
            XMLConfiguration conf = new XMLConfiguration(url);
            loadMap(conf);
        } catch (ConfigurationException e) {
            throw new ProfileConfException(PROFILES_FILE_LOAD_ERR, url.getFile(), String.valueOf(e.getCause()));
        }
    }

    private void loadMap(XMLConfiguration conf) {
        String[] profileNames = conf.getStringArray("profile.name");
        if (profileNames.length == 0) {
            LOG.warn("Profile file: " + conf.getFileName() + " is empty");
            return;
        }
        Map<String, Map<String, String>> profileMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int profileIdx = 0; profileIdx < profileNames.length; profileIdx++) {
            String profileName = profileNames[profileIdx];
            if (profileMap.containsKey(profileName)) {
                LOG.warn("Duplicate profile definition found in " + conf.getFileName() + " for: " + profileName);
                continue;
            }
            Configuration profileSubset = conf.subset("profile(" + profileIdx + ").plugins");
            profileMap.put(profileName, getProfilePluginMap(profileSubset));
        }
        profilesMap.putAll(profileMap);
    }

    private Map<String, String> getProfilePluginMap(Configuration profileSubset) {
        @SuppressWarnings("unchecked") //IteratorUtils doesn't yet support generics.
        List<String> plugins = IteratorUtils.toList(profileSubset.getKeys());
        Map<String, String> pluginsMap = new HashMap<>();
        for (String plugin : plugins) {
            String pluginValue = profileSubset.getString(plugin);
            if (!StringUtils.isEmpty(StringUtils.trim(pluginValue))) {
                pluginsMap.put(InputData.USER_PROP_PREFIX + plugin.toUpperCase(), pluginValue);
            }
        }
        return pluginsMap;
    }
}
