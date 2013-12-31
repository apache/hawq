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
		private String 	sourceName;	// File path+name, table name, etc.
		private int		index;		// Fragment index (incremented per sourceName)
		private String[] replicas;	// Fragment replicas (1 or more)
		private byte[] 	metadata;	// Fragment metadata information 
									//(starting point + length, region location, etc.)

		private byte[] userData;	// ThirdParty data added to a fragment. Ignored if null
		
		public FragmentInfo(String   sourceName,
							String[] hosts,
							byte[] metadata)
		{
			this.sourceName	= sourceName;
			this.replicas		= hosts;
			this.metadata	= metadata;
		}
		
		public FragmentInfo(String sourceName,
							String[] hosts,
							byte[] metadata,
							byte[] userData)
		{
			this.sourceName	= sourceName;
			this.replicas		= hosts;
			this.metadata	= metadata;
			this.userData   = userData;
		}
		
		public String getSourceName()
		{
			return this.sourceName;
		}
		
		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String[] getReplicas()
		{
			return this.replicas;
		}
		
		public void setReplicas(String[] replicas) {
			this.replicas = replicas;
		}
		
		public byte[] getMetadata() {
			return metadata;
		}

		public void setMetadata(byte[] metadata) {
			this.metadata = metadata;
		}

		public byte[] getUserData()
		{
			return this.userData;
		}

		public void setUserData(byte[] data)
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
							String[] replicas,
							byte[] metadata)
	{
		fragments.add(new FragmentInfo(sourceName, replicas, metadata));
	}
	
	public void addFragment(String sourceName,
							String[] replicas,
							byte[] metadata,
							byte[] userData)
	{
		fragments.add(new FragmentInfo(sourceName, replicas, metadata, userData));
	}
	
	public List<FragmentInfo> getFragments()
	{
		return fragments;
	}
	
	
}
