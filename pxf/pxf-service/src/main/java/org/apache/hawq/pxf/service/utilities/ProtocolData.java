package org.apache.hawq.pxf.service.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.ProfilesConf;

/**
 * Common configuration of all MetaData classes. Provides read-only access to
 * common parameters supplied using system properties.
 */
public class ProtocolData extends InputData {

    private static final String TRUE_LCASE = "true";
    private static final String FALSE_LCASE = "false";
    private static final String PROP_PREFIX = "X-GP-";
    public static final int INVALID_SPLIT_IDX = -1;

    private static final Log LOG = LogFactory.getLog(ProtocolData.class);

    protected OutputFormat outputFormat;
    protected int port;
    protected String host;
    protected String profile;
    protected String token;
    // statistics parameters
    protected int statsMaxFragments;
    protected float statsSampleRatio;

    /**
     * Constructs a ProtocolData. Parses X-GP-* configuration variables.
     *
     * @param paramsMap contains all query-specific parameters from Hawq
     */
    public ProtocolData(Map<String, String> paramsMap) {

        requestParametersMap = paramsMap;
        segmentId = getIntProperty("SEGMENT-ID");
        totalSegments = getIntProperty("SEGMENT-COUNT");
        filterStringValid = getBoolProperty("HAS-FILTER");

        if (filterStringValid) {
            filterString = getProperty("FILTER");
        }

        parseFormat(getProperty("FORMAT"));

        host = getProperty("URL-HOST");
        port = getIntProperty("URL-PORT");

        tupleDescription = new ArrayList<ColumnDescriptor>();
        recordkeyColumn = null;
        parseTupleDescription();

        /*
         * accessor - will throw exception from getPropery() if outputFormat is
         * BINARY and the user did not supply accessor=... or profile=...
         * resolver - will throw exception from getPropery() if outputFormat is
         * BINARY and the user did not supply resolver=... or profile=...
         */
        profile = getOptionalProperty("PROFILE");
        if (profile != null) {
            setProfilePlugins();
        }
        accessor = getProperty("ACCESSOR");
        resolver = getProperty("RESOLVER");
        analyzer = getOptionalProperty("ANALYZER");
        fragmenter = getOptionalProperty("FRAGMENTER");
        dataSource = getProperty("DATA-DIR");

        /* Kerberos token information */
        if (UserGroupInformation.isSecurityEnabled()) {
            token = getProperty("TOKEN");
        }

        parseFragmentMetadata();
        parseUserData();
        parseThreadSafe();
        parseRemoteCredentials();

        dataFragment = INVALID_SPLIT_IDX;
        parseDataFragment(getOptionalProperty("DATA-FRAGMENT"));

        statsMaxFragments = 0;
        statsSampleRatio = 0;
        parseStatsParameters();

        // Store alignment for global use as a system property
        System.setProperty("greenplum.alignment", getProperty("ALIGNMENT"));
    }

    /**
     * Constructs an InputDataBuilder from a copy. Used to create from an
     * extending class.
     *
     * @param copy the input data to copy
     */
    public ProtocolData(ProtocolData copy) {
        this.requestParametersMap = copy.requestParametersMap;
        this.segmentId = copy.segmentId;
        this.totalSegments = copy.totalSegments;
        this.outputFormat = copy.outputFormat;
        this.host = copy.host;
        this.port = copy.port;
        this.fragmentMetadata = copy.fragmentMetadata;
        this.userData = copy.userData;
        this.tupleDescription = copy.tupleDescription;
        this.recordkeyColumn = copy.recordkeyColumn;
        this.filterStringValid = copy.filterStringValid;
        this.filterString = copy.filterString;
        this.dataSource = copy.dataSource;
        this.accessor = copy.accessor;
        this.resolver = copy.resolver;
        this.fragmenter = copy.fragmenter;
        this.analyzer = copy.analyzer;
        this.threadSafe = copy.threadSafe;
        this.remoteLogin = copy.remoteLogin;
        this.remoteSecret = copy.remoteSecret;
        this.token = copy.token;
        this.statsMaxFragments = copy.statsMaxFragments;
        this.statsSampleRatio = copy.statsSampleRatio;
    }

    /**
     * Sets the requested profile plugins from profile file into
     * {@link #requestParametersMap}.
     */
    private void setProfilePlugins() {
        Map<String, String> pluginsMap = ProfilesConf.getProfilePluginsMap(profile);
        checkForDuplicates(pluginsMap, requestParametersMap);
        requestParametersMap.putAll(pluginsMap);
    }

    /**
     * Verifies there are no duplicates between parameters declared in the table
     * definition and parameters defined in a profile.
     *
     * The parameters' names are case insensitive.
     */
    private void checkForDuplicates(Map<String, String> plugins,
                                    Map<String, String> params) {
        List<String> duplicates = new ArrayList<>();
        for (String key : plugins.keySet()) {
            if (params.containsKey(key)) {
                duplicates.add(key);
            }
        }

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Profile '" + profile
                    + "' already defines: "
                    + String.valueOf(duplicates).replace("X-GP-", ""));
        }
    }

    /**
     * Returns the request parameters.
     *
     * @return map of request parameters
     */
    public Map<String, String> getParametersMap() {
        return requestParametersMap;
    }

    /**
     * Throws an exception when the given property value is missing in request.
     *
     * @param property missing property name
     * @throws IllegalArgumentException throws an exception with the property
     *             name in the error message
     */
    public void protocolViolation(String property) {
        String error = "Internal server error. Property \"" + property
                + "\" has no value in current request";

        LOG.error(error);
        throw new IllegalArgumentException(error);
    }

    /**
     * Returns the value to which the specified property is mapped in
     * {@link #requestParametersMap}.
     *
     * @param property the lookup property key
     * @throws IllegalArgumentException if property key is missing
     */
    private String getProperty(String property) {
        String result = requestParametersMap.get(PROP_PREFIX + property);

        if (result == null) {
            protocolViolation(property);
        }

        return result;
    }

    /**
     * Returns the optional property value. Unlike {@link #getProperty}, it will
     * not fail if the property is not found. It will just return null instead.
     *
     * @param property the lookup optional property
     * @return property value as a String
     */
    private String getOptionalProperty(String property) {
        return requestParametersMap.get(PROP_PREFIX + property);
    }

    /**
     * Returns a property value as an int type.
     *
     * @param property the lookup property
     * @return property value as an int type
     * @throws NumberFormatException if the value is missing or can't be
     *             represented by an Integer
     */
    private int getIntProperty(String property) {
        return Integer.parseInt(getProperty(property));
    }

    /**
     * Returns a property value as boolean type. A boolean property is defined
     * as an int where 0 means false, and anything else true (like C).
     *
     * @param property the lookup property
     * @return property value as boolean
     * @throws NumberFormatException if the value is missing or can't be
     *             represented by an Integer
     */
    private boolean getBoolProperty(String property) {
        return getIntProperty(property) != 0;
    }

    /**
     * Returns the current output format, either {@link OutputFormat#TEXT} or
     * {@link OutputFormat#BINARY}.
     *
     * @return output format
     */
    public OutputFormat outputFormat() {
        return outputFormat;
    }

    /**
     * Returns the server name providing the service.
     *
     * @return server name
     */
    public String serverName() {
        return host;
    }

    /**
     * Returns the server port providing the service.
     *
     * @return server port
     */
    public int serverPort() {
        return port;
    }

    /**
     * Returns Kerberos token information.
     *
     * @return token
     */
    public String getToken() {
        return token;
    }

    /**
     * Statistics parameter. Returns the max number of fragments to return for
     * ANALYZE sampling. The value is set in HAWQ side using the GUC
     * pxf_stats_max_fragments.
     *
     * @return max number of fragments to be processed by analyze
     */
    public int getStatsMaxFragments() {
        return statsMaxFragments;
    }

    /**
     * Statistics parameter. Returns a number between 0.0001 and 1.0,
     * representing the sampling ratio on each fragment for ANALYZE sampling.
     * The value is set in HAWQ side based on ANALYZE computations and the
     * number of sampled fragments.
     *
     * @return sampling ratio
     */
    public float getStatsSampleRatio() {
        return statsSampleRatio;
    }

    /**
     * Sets the thread safe parameter. Default value - true.
     */
    private void parseThreadSafe() {

        threadSafe = true;
        String threadSafeStr = getOptionalProperty("THREAD-SAFE");
        if (threadSafeStr != null) {
            threadSafe = parseBooleanValue(threadSafeStr);
        }
    }

    private boolean parseBooleanValue(String threadSafeStr) {

        if (threadSafeStr.equalsIgnoreCase(TRUE_LCASE)) {
            return true;
        }
        if (threadSafeStr.equalsIgnoreCase(FALSE_LCASE)) {
            return false;
        }
        throw new IllegalArgumentException("Illegal boolean value '"
                + threadSafeStr + "'." + " Usage: [TRUE|FALSE]");
    }

    /**
     * Sets the format type based on the input string. Allowed values are:
     * "TEXT", "GPDBWritable".
     *
     * @param formatString format string
     */
    protected void parseFormat(String formatString) {
        switch (formatString) {
            case "TEXT":
                outputFormat = OutputFormat.TEXT;
                break;
            case "GPDBWritable":
                outputFormat = OutputFormat.BINARY;
                break;
            default:
                throw new IllegalArgumentException(
                        "Wrong value for greenplum.format " + formatString);
        }
    }

    /*
     * Sets the tuple description for the record
     */
    void parseTupleDescription() {
        int columns = getIntProperty("ATTRS");
        for (int i = 0; i < columns; ++i) {
            String columnName = getProperty("ATTR-NAME" + i);
            int columnTypeCode = getIntProperty("ATTR-TYPECODE" + i);
            String columnTypeName = getProperty("ATTR-TYPENAME" + i);

            ColumnDescriptor column = new ColumnDescriptor(columnName,
                    columnTypeCode, i, columnTypeName);
            tupleDescription.add(column);

            if (columnName.equalsIgnoreCase(ColumnDescriptor.RECORD_KEY_NAME)) {
                recordkeyColumn = column;
            }
        }
    }

    /**
     * Sets the index of the allocated data fragment
     *
     * @param fragment the allocated data fragment
     */
    protected void parseDataFragment(String fragment) {

        /*
         * Some resources don't require a fragment, hence the list can be empty.
         */
        if (StringUtils.isEmpty(fragment)) {
            return;
        }
        dataFragment = Integer.parseInt(fragment);
    }

    private void parseFragmentMetadata() {
        fragmentMetadata = parseBase64("FRAGMENT-METADATA",
                "Fragment metadata information");
    }

    private void parseUserData() {
        userData = parseBase64("FRAGMENT-USER-DATA", "Fragment user data");
    }

    private byte[] parseBase64(String key, String errName) {
        String encoded = getOptionalProperty(key);
        if (encoded == null) {
            return null;
        }
        if (!Base64.isArrayByteBase64(encoded.getBytes())) {
            throw new IllegalArgumentException(errName
                    + " must be Base64 encoded." + "(Bad value: " + encoded
                    + ")");
        }
        byte[] parsed = Base64.decodeBase64(encoded);
        LOG.debug("decoded " + key + ": " + new String(parsed));
        return parsed;
    }

    private void parseRemoteCredentials() {
        remoteLogin = getOptionalProperty("REMOTE-USER");
        remoteSecret = getOptionalProperty("REMOTE-PASS");
    }

    private void parseStatsParameters() {

        String maxFrags = getOptionalProperty("STATS-MAX-FRAGMENTS");
        if (!StringUtils.isEmpty(maxFrags)) {
            statsMaxFragments = Integer.parseInt(maxFrags);
            if (statsMaxFragments <= 0) {
                throw new IllegalArgumentException("Wrong value '"
                        + statsMaxFragments + "'. "
                        + "STATS-MAX-FRAGMENTS must be a positive integer");
            }
        }

        String sampleRatioStr = getUserProperty("STATS-SAMPLE-RATIO");
        if (!StringUtils.isEmpty(sampleRatioStr)) {
            statsSampleRatio = Float.parseFloat(sampleRatioStr);
            if (statsSampleRatio < 0.0001 || statsSampleRatio > 1.0) {
                throw new IllegalArgumentException(
                        "Wrong value '"
                                + statsSampleRatio
                                + "'. "
                                + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
            }
        }

        if ((statsSampleRatio > 0) != (statsMaxFragments > 0)) {
            throw new IllegalArgumentException(
                    "Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        }
    }
}
