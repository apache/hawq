package org.apache.hawq.pxf.service;

import org.apache.hawq.pxf.api.Fragment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

/**
 * Utility class for converting Fragments into a {@link FragmentsResponse}
 * that will serialize them into JSON format.
 */
public class FragmentsResponseFormatter {

    private static Log LOG = LogFactory.getLog(FragmentsResponseFormatter.class);

    /**
     * Converts Fragments list to FragmentsResponse
     * after replacing host name by their respective IPs.
     *
     * @param fragments list of fragments
     * @param data data (e.g. path) related to the fragments
     * @return FragmentsResponse with given fragments
     * @throws UnknownHostException if converting host names to IP fails
     */
    public static FragmentsResponse formatResponse(List<Fragment> fragments, String data) throws UnknownHostException   {
        /* print the raw fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            LOG.debug("Fragments before conversion to IP list:");
            FragmentsResponseFormatter.printList(fragments, data);
        }

        /* HD-2550: convert host names to IPs */
        convertHostsToIPs(fragments);

        updateFragmentIndex(fragments);

	/* print the fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            FragmentsResponseFormatter.printList(fragments, data);
        }

        return new FragmentsResponse(fragments);
    }

    /**
     * Updates the fragments' indexes so that it is incremented by sourceName.
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

    /**
     * Converts hosts to their matching IP addresses.
     *
     * @throws UnknownHostException if converting host name to IP fails
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
     * Converts a fragments list to a readable string and prints it to the log.
     * Intended for debugging purposes only.
     * 'datapath' is the data path part of the original URI (e.g., table name, *.csv, etc).
	 */
    private static void printList(List<Fragment> fragments, String datapath) {
        LOG.debug("List of " +
                (fragments.isEmpty() ? "no" : fragments.size()) + "fragments for \"" +
                 datapath + "\"");

        int i = 0;
        for (Fragment fragment : fragments) {
            StringBuilder result = new StringBuilder();
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
            LOG.debug(result);
        }
    }
}
