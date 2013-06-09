package com.pivotal.pxf.fragmenters;

import java.util.ArrayList;
import java.util.List;

/*
 * Class holding information about fragments (FragmentInfo)
 */
public class FragmentsOutput {

	/*
	 * Fragmenter Info is a public class that represents the information of
	 * a data fragment. It is to be used with any GP Fusion Fragmenter impl.
	 */
	class FragmentInfo
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
		
		public FragmentInfo(String sourceName,
							String[] hosts,
							String userData)
		{
			this.sourceName	= sourceName;
			this.hosts		= hosts;
			this.userData   = userData;
		}
		
		public String getSourceName()
		{
			return this.sourceName;
		}
		
		public String[] getHosts()
		{
			return this.hosts;
		}
		
		public void setHosts(String[] hosts) {
			this.hosts = hosts;
		}
		
		public String getUserData()
		{
			return this.userData;
		}

		public void setUserData(String data)
		{
			userData = data;
		}	
		
	}
	
	private List<FragmentInfo> fragments;
	
	public FragmentsOutput()
	{
		fragments = new ArrayList<FragmentInfo>();
	}
	
	public void addFragment(String   sourceName,
							String[] hosts)
	{
		fragments.add(new FragmentInfo(sourceName, hosts));
	}
	
	public void addFragment(String sourceName,
							String[] hosts,
							String userData)
	{
		fragments.add(new FragmentInfo(sourceName, hosts, userData));
	}
	
	public List<FragmentInfo> getFragments()
	{
		return fragments;
	}
	
	
}
