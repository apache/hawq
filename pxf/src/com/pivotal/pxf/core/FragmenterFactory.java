package com.pivotal.pxf.core;

import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.core.utilities.Utilities;

/*
 * Factory class for creation of Fragmenter objects. The actual Fragmenter object is "hidden" behind
 * an Fragmenter abstract class which is returned by the FragmenterFactory. 
 */
public class FragmenterFactory {
    static public Fragmenter create(InputData conf) throws Exception {
        String fragmenterName = conf.getProperty("X-GP-FRAGMENTER");
        return (Fragmenter) Utilities.createAnyInstance(InputData.class, fragmenterName, conf);
    }
}
