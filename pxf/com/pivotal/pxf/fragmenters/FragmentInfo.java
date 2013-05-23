package com.pivotal.pxf.fragmenters;

import java.io.IOException;
import java.lang.StringBuilder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

/*
 * Fragmenter Info is a public class that represents the information of
 * a data fragment. It is to be used with any GP Fusion Fragmenter impl.
 */
public class FragmentInfo
{	
	private String sourceName;	// File path+name, table name, etc.	
	private String[] hosts;	    // Fragment hostnames (1 or more)
	private String userData;	// ThirdParty data added to a fragment. Ignored if null
	
	public FragmentInfo(String   sourceName,
						String[] hosts)
	{
		this.sourceName	= sourceName;
		this.hosts		= hosts;
	}
	
	public void setUserData(String data)
	{
		userData = data;
	}			
	
	public String getSourceName()
	{
		return this.sourceName;
	}
	
	public String[] getHosts()
	{
		return this.hosts;
	}
	
	public String getUserData()
	{
		return this.userData;
	}

	/*
	 * convert hosts to their matching IP addresses
	 */
	public static void convertHostsToIPs(List<FragmentInfo> fragments) throws UnknownHostException
	{
		/* host converted to IP map. Used to limit network calls. */
		HashMap<String, String> hostToIpMap = new HashMap<String,String>();
		
		for (FragmentInfo fragment : fragments)
		{
			String[] hosts = fragment.getHosts();
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
			fragment.hosts = ips;
		}	
	}
	
	/*
	 * Given a list of FragmentInfos, serialize it in JSON to be used as
	 * the result string for GPDB. An example result is as follows:
	 *
	 * {"GPXFFragments":[{"hosts":["sdw1.corp.emc.com","sdw3.corp.emc.com","sdw8.corp.emc.com"],"sourceName":"text2.csv","userData":"<data_specific_to_third_party_fragmenter>"},{"hosts":["sdw2.corp.emc.com","sdw4.corp.emc.com","sdw5.corp.emc.com"],"sourceName":"text_data.csv","userData":"<data_specific_to_third_party_fragmenter>"}]}
	 */
	public static String listToJSON(List<FragmentInfo> fragmentInfos) throws IOException
	{
		ObjectMapper	mapper	= new ObjectMapper();
		
		String			result	= new String("{\"GPXFFragments\":[");
		boolean			isFirst	= true;
		
		for (FragmentInfo fi : fragmentInfos)
		{
			if (!isFirst)
				result += ",";
			
			result += mapper.writeValueAsString(fi);
			isFirst = false;
		}
		
		result += "]}";
		
		return result;
	}
	
	/*
	 * Given a list of FragmentInfos, convert it to be readable. Intended
	 * for debugging purposes only. 'datapath' is the data path part of 
	 * the original URI (e.g., table name, *.csv, etc). 
	 */
	public static String listToString(List<FragmentInfo> fragmentInfos, String datapath)
	{
		StringBuilder result = new StringBuilder();
		
		result.append("List of fragments for \"" + datapath + "\": ");
		int i = 0;
		
		for (FragmentInfo fi : fragmentInfos)
		{
			++i;
			result.append("Fragment #" + i + ": [");
			result.append("Source: " + fi.sourceName + ", Hosts:");
			
			for (String host : fi.hosts)
				result.append(" " + host);
			
			result.append(", User Data: " + fi.userData);
			result.append("] ");
		}
		if (fragmentInfos.isEmpty())
				result.append("no fragments");
		
		return result.toString();
	}
	
}
