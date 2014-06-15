package com.pivotal.pxf.service;

import com.pivotal.pxf.api.Fragment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

/*
 * Utility class for converting FragmentsOutput into a JSON format.
 */
public class FragmentsResponseFormatter {

    private static Log Log = LogFactory.getLog(FragmentsResponseFormatter.class);

    /*
     * Convert FragmentsOutput to JSON String format,
     * after replacing host name by their respective IPs.
     *
     */
    public static String formatResponseString(List<Fragment> fragments, String data) throws IOException {
        /* print the raw fragment list to log when in debug level */
        Log.debug("Fragments before conversion to IP list: " +
                FragmentsResponseFormatter.listToString(fragments, data));

		/* HD-2550: convert host names to IPs */
        convertHostsToIPs(fragments);

        updateFragmentIndex(fragments);

		/* print the fragment list to log when in debug level */
        Log.debug(FragmentsResponseFormatter.listToString(fragments, data));

        return FragmentsResponseFormatter.listToJSON(fragments);
    }

    /**
     * Update the fragments' indexes so that it is incremented by sourceName.
     * (E.g.: {"a", 0}, {"a", 1}, {"b", 0} ... )
     *
     * @param fragments fragments to be updated
     */
    private static void updateFragmentIndex(List<Fragment> fragments) {

        String sourceName = null;
        int index = 0;
        for (Fragment fragment : fragments) {

            String currentSourceName = fragment.getSourceName();
            if (!currentSourceName.equals(sourceName)) {
                index = 0;
                sourceName = currentSourceName;
            }
            fragment.setIndex(index++);
        }
    }

    /*
     * convert hosts to their matching IP addresses
     */
    private static void convertHostsToIPs(List<Fragment> fragments) throws UnknownHostException {
        /* host converted to IP map. Used to limit network calls. */
        HashMap<String, String> hostToIpMap = new HashMap<String, String>();

        for (Fragment fragment : fragments) {
            String[] hosts = fragment.getReplicas();
            if (hosts == null) {
                continue;
            }
            String[] ips = new String[hosts.length];
            int index = 0;

            for (String host : hosts) {
                String convertedIp = hostToIpMap.get(host);
                if (convertedIp == null) {
                    /* find host's IP, and add to map */
                    InetAddress addr = InetAddress.getByName(host);
                    convertedIp = addr.getHostAddress();
                    hostToIpMap.put(host, convertedIp);
                }

				/* update IPs array */
                ips[index] = convertedIp;
                ++index;
            }
            fragment.setReplicas(ips);
        }
    }

    /*
     * Serializes a fragments list in JSON,
     * To be used as the result string for GPDB.
     * An example result is as follows:
     * {"PXFFragments":[{"replicas":["sdw1.corp.emc.com","sdw3.corp.emc.com","sdw8.corp.emc.com"],"sourceName":"text2.csv", "index":"0", "metadata":<base64 metadata for fragment>, "userData":"<data_specific_to_third_party_fragmenter>"},{"replicas":["sdw2.corp.emc.com","sdw4.corp.emc.com","sdw5.corp.emc.com"],"sourceName":"text_data.csv","index":"0","metadata":<base64 metadata for fragment>,"userData":"<data_specific_to_third_party_fragmenter>"}]}
     */
    private static String listToJSON(List<Fragment> fragments) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        StringBuilder result = new StringBuilder("{\"PXFFragments\":[");
        String prefix = "";
        for (Fragment fragment : fragments) {
            /* metaData and userData are automatically converted to Base64 */
            result.append(prefix).append(mapper.writeValueAsString(fragment));
            prefix = ",";
        }
        return result.append("]}").toString();
    }

    /*
     * Converts a fragments list to a readable string.
     * Intended for debugging purposes only.
     * 'datapath' is the data path part of the original URI (e.g., table name, *.csv, etc).
	 */
    private static String listToString(List<Fragment> fragments, String datapath) {
        StringBuilder result = new StringBuilder("List of fragments for \"");
        result.append(datapath).append("\": ");

        if (fragments.isEmpty()) {
            return result.append("no fragments").toString();
        }

        int i = 0;
        for (Fragment fragment : fragments) {
            result.append("Fragment #").append(++i).append(": [")
                    .append("Source: ").append(fragment.getSourceName())
                    .append(", Index: ").append(fragment.getIndex())
                    .append(", Replicas:");
            for (String host : fragment.getReplicas()) {
                result.append(" ").append(host);
            }

            result.append(", Metadata: ").append(new String(fragment.getMetadata()));

            if (fragment.getUserData() != null) {
                result.append(", User Data: ").append(new String(fragment.getUserData()));
            }
            result.append("] ");
        }

        return result.toString();
    }
}
