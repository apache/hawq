package com.emc.greenplum.gpdb.hdfsconnector;

/*
 * Factory class for creation of Bridge objects. The actual Bridge object is "hidden" behind
 * an IBridge interface which is returned by the BridgeFactory. The Bridge user knows only
 * the interface and does not know the actual Bridge that it manipulates. The factory decides
 * which type of bridge to create based on the parameters fileType and serializationMetod
 * which are passed in the commonConf
 */
public class BridgeFactory
{
	static public IBridge create(HDMetaData commonConf) throws Exception
	{
		IBridge bridge = null;

        switch(commonConf.protocol())
        {
            case GPHDFS:
                bridge = new GPHdfsBridge(new HDFSMetaData(commonConf));
                break;

            case GPHBASE:
                bridge = new HBaseBridge(HBaseLookupTable.translateColumns(new HBaseMetaData(commonConf)));
                break;
        }

		return bridge;
	}
}
