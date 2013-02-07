package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import java.lang.StringBuilder;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.annotate.JsonView;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/*
 * View classes are a JSON tool, used to filter which FragmentInfo fields will be serialized
 */
class Views 
{
	/* 
	 * FragmentInfo fields that belong to this view are the basic properties that are always needed
	 */
	static class FusionView { } 
	/* 
	 * Controls serialization of ThirdParty fields - additional data that a user wants to attach to a fragment
	 */	
	static class ThirdPartyView extends FusionView { }
}

/*
 * Fragmenter Info is a public class that represents the information of
 * a data fragment. It is to be used with any GP Fusion Fragmenter impl.
 */
public class FragmentInfo
{	
	@JsonView(Views.FusionView.class) private String     sourceName;	// File path+name, table name, etc.	
	@JsonView(Views.FusionView.class) private String[]   hosts;	    // Fragment hostnames (1 or more)
	@JsonView(Views.ThirdPartyView.class) private String userData;  // ThirdParty data added to a fragment
	
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
	 * Given a list of FragmentInfos, serialize it in JSON to be used as
	 * the result string for GPDB. An example result is as follows:
	 *
	 * {"GPXFFragments":[{"hosts":["sdw1.corp.emc.com","sdw3.corp.emc.com","sdw8.corp.emc.com"],"sourceName":"text2.csv","userData":"<data_specific_to_third_party_fragmenter>"},{"hosts":["sdw2.corp.emc.com","sdw4.corp.emc.com","sdw5.corp.emc.com"],"sourceName":"text_data.csv","userData":"<data_specific_to_third_party_fragmenter>"}]}
	 */
	public static String listToJSON(List<FragmentInfo> fragmentInfos) throws IOException
	{
		ObjectMapper	mapper	= new ObjectMapper();
		if (fragmentInfos.get(0).getUserData() == null) // semi-ugly assumption. If the first fragment has ThirdParty data, then all fragments will have it
			mapper.getSerializationConfig().withView(Views.FusionView.class);
		else
			mapper.getSerializationConfig().withView(Views.ThirdPartyView.class);
		
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
		
		result.append("List of fragments for \"" + datapath + "\" ");
		
		for (FragmentInfo fi : fragmentInfos)
		{
			result.append("Fragment: [");
			result.append("Source: " + fi.sourceName + ", Hosts: ");
			
			for (String host : fi.hosts)
				result.append(host + " ");
			
			result.append("]");
		}
				
		return result.toString();
	}
	
}
