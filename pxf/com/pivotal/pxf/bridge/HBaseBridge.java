package com.pivotal.pxf.bridge;

import com.pivotal.pxf.accessors.HBaseAccessor;
import com.pivotal.pxf.resolvers.HBaseResolver;
import com.pivotal.pxf.utilities.HBaseMetaData;

public class HBaseBridge extends BasicBridge
{
    public HBaseBridge(HBaseMetaData conf) throws Exception
    {
        super (conf,
               new HBaseAccessor(conf),
               new HBaseResolver(conf));
    }
}
