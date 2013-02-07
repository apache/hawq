package com.emc.greenplum.gpdb.hdfsconnector;

public abstract class BaseFileAccessor implements IHdfsFileAccessor
{
	protected Object recordkey = null;
	
	/*
	 * Currently, the only  method implemented from the interface IHdfsFileAccessor
	 */
	public Object getRecordkey()
	{
		return recordkey;
	}

}