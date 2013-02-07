package com.emc.greenplum.gpdb.hdfsconnector;

public class HBaseBridge extends BasicBridge
{
    public HBaseBridge(HBaseMetaData conf) throws Exception
    {
        super (conf,
               new HBaseAccessor(conf),
               new HBaseResolver(conf));
    }
}
