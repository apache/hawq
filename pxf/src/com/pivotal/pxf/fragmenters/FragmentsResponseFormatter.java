package com.pivotal.pxf.fragmenters;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.pivotal.pxf.fragmenters.FragmentsOutput.FragmentInfo;

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
	public static String formatResponseString(FragmentsOutput fragments, String data) throws IOException
	{		
		/* print the raw fragment list to log when in debug level */
		Log.debug("Fragments before conversion to IP list: " +
				  FragmentsResponseFormatter.listToString(fragments, data));

		/* HD-2550: convert host names to IPs */
		FragmentsResponseFormatter.convertHostsToIPs(fragments);

		FragmentsResponseFormatter.updateFragmentIndex(fragments);
		
		/* print the fragment list to log when in debug level */
		Log.debug(FragmentsResponseFormatter.listToString(fragments, data));
		
		return FragmentsResponseFormatter.listToJSON(fragments);
	}

	/**
	 * Update the fragments' indexes so that it is incremented by sourceName.
	 * (E.g.: {"a", 0}, {"a", 1}, {"b", 0} ... )
	 * @param fragments fragments to be updated
	 */
	private static void updateFragmentIndex(FragmentsOutput fragments) {
		
		String sourceName = null;
		int index = 0;
		List<FragmentInfo> fragmentInfos = fragments.getFragments();
		
		for (FragmentInfo fragment : fragmentInfos) {
			
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
	private static void convertHostsToIPs(FragmentsOutput fragments) throws UnknownHostException
	{
		/* host converted to IP map. Used to limit network calls. */
		HashMap<String, String> hostToIpMap = new HashMap<String,String>();
		List<FragmentInfo> fragmentInfos = fragments.getFragments();

		for (FragmentInfo fragment : fragmentInfos)
		{
			String[] hosts = fragment.getReplicas();
			if (hosts == null)
				continue;
			String[] ips = new String[hosts.length];
			int index = 0;

			for (String host : hosts)
			{
				String convertedIp = hostToIpMap.get(host);
				if (convertedIp == null)
				{
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
	 * Given a FragmentsOutput object, serialize it in JSON to be used as
	 * the result string for GPDB. An example result is as follows:
	 *
	 * {"PXFFragments":[{"replicas":["sdw1.corp.emc.com","sdw3.corp.emc.com","sdw8.corp.emc.com"],"sourceName":"text2.csv", "index":"0", "metadata":<base64 metadata for fragment>, "userData":"<data_specific_to_third_party_fragmenter>"},{"replicas":["sdw2.corp.emc.com","sdw4.corp.emc.com","sdw5.corp.emc.com"],"sourceName":"text_data.csv","index":"0","metadata":<base64 metadata for fragment>,"userData":"<data_specific_to_third_party_fragmenter>"}]}
	 */
	private static String listToJSON(FragmentsOutput fragments) throws IOException
	{
		ObjectMapper	mapper	= new ObjectMapper();

		String			result	= new String("{\"PXFFragments\":[");
		boolean			isFirst	= true;
		List<FragmentInfo> fragmentInfos = fragments.getFragments();

		for (FragmentInfo fi : fragmentInfos)
		{
			if (!isFirst)
				result += ",";

			/* userData is automatically converted to Base64 */
			result += mapper.writeValueAsString(fi);
			isFirst = false;
		}

		result += "]}";

		return result;
	}
	
	/*
	 * Given a FragmentsOutput object, convert it to be readable. Intended
	 * for debugging purposes only. 'datapath' is the data path part of 
	 * the original URI (e.g., table name, *.csv, etc). 
	 */
	private static String listToString(FragmentsOutput fragments, String datapath)
	{
		StringBuilder result = new StringBuilder();

		result.append("List of fragments for \"" + datapath + "\": ");
		int i = 0;

		List<FragmentInfo> fragmentInfos = fragments.getFragments();
		for (FragmentInfo fi : fragmentInfos)
		{
			++i;
			result.append("Fragment #" + i + ": [");
			result.append("Source: " + fi.getSourceName() + 
			              ", index: " + fi.getIndex() + ", Replicas:");

			for (String host : fi.getReplicas())
				result.append(" " + host);

			result.append(", Metadata: " + fi.getMetadata());
			
			if (fi.getUserData() != null)
				result.append(", User Data: " + new String(fi.getUserData()));
			result.append("] ");
		}
		if (fragmentInfos.isEmpty())
			result.append("no fragments");
		
		return result.toString();
	}
}
