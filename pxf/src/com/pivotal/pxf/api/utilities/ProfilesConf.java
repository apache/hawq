package com.pivotal.pxf.api.utilities;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pivotal.pxf.api.utilities.ProfileConfException.MessageFormat.*;
import static com.pivotal.pxf.api.utilities.ProfileConfException.MessageFormat;


/**
 * This enum holds the profiles files: pxf-profiles.xml and pxf-profiles-default.xml.
 * It exposes a public static method getProfilePluginsMap(String plugin) which returns the requested profile plugins
 */
public enum ProfilesConf {
    PROFILES_CONF("pxf-profiles.xml"),
    PROFILES_CONF_DEFAULT("resources/pxf-profiles-default.xml");
    private static final Log LOG = LogFactory.getLog(ProfilesConf.class);
    private String fileName;
    private URL url;
    private XMLConfiguration conf;

    ProfilesConf(String fileName) {
        this.fileName = fileName;
        url = getClassLoader().getResource(fileName);
    }

    private static ClassLoader getClassLoader() {
        ClassLoader cL = Thread.currentThread().getContextClassLoader();
        return (cL != null)
                ? cL
                : ProfilesConf.class.getClassLoader();
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
        try {
            return PROFILES_CONF.getPluginsMap(profile);
        } catch (ProfileConfException pce) {
            if (!useProfilesDefaults(pce.getMsgFormat())) {
                throw pce;
            }
            LOG.debug(pce.getMessage() + ". Using profiles defaults");
            return PROFILES_CONF_DEFAULT.getPluginsMap(profile);
        }
    }

    private static boolean useProfilesDefaults(MessageFormat msg) {
        return msg == PROFILES_FILE_NOT_FOUND || msg == NO_PROFILE_DEF;
    }

    private XMLConfiguration getConf() {
        if (url == null) {
            throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, fileName);
        }
        if (conf == null || conf.isEmpty()) {
            conf = loadConf();
        }
        return conf;
    }

    private XMLConfiguration loadConf() {
        XMLConfiguration conf;
        try {
            conf = new XMLConfiguration(url);
            conf.setReloadingStrategy(new FileChangedReloadingStrategy());
        } catch (ConfigurationException e) {
            throw new ProfileConfException(PROFILES_FILE_LOAD_ERR, fileName, String.valueOf(e.getCause()));
        }
        return conf;
    }

    private Map<String, String> getPluginsMap(String profile) {
        Configuration profileSubset = getProfileSubset(profile);
        @SuppressWarnings("unchecked") //IteratorUtils doesn't yet support generics.
        List<String> plugins = IteratorUtils.toList(profileSubset.getKeys());
        Map<String, String> pluginsMap = new HashMap<String, String>();
        for (String plugin : plugins) {
            String pluginValue = profileSubset.getString(plugin);
            if (!StringUtils.isEmpty(StringUtils.trim(pluginValue))) {
                pluginsMap.put("X-GP-" + plugin.toUpperCase(), pluginValue);
            }
        }
        return pluginsMap;
    }

    private Configuration getProfileSubset(String profile) {
        String[] profileNames = getConf().getStringArray("profile.name");
        for (int profileIdx = 0; profileIdx < profileNames.length; profileIdx++) {
            if (profileNames[profileIdx].equalsIgnoreCase(profile)) {
                return getConf().subset("profile(" + profileIdx + ").plugins");
            }
        }
        throw new ProfileConfException(NO_PROFILE_DEF, profile, PROFILES_CONF.fileName);
    }
}
