package com.pivotal.pxf.service;

import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.service.utilities.Utilities;

/**
 * Factory class for creation of {@link Fragmenter} objects. The actual {@link Fragmenter} object is "hidden" behind
 * an {@link Fragmenter} abstract class which is returned by the FragmenterFactory. 
 */
public class FragmenterFactory {
    static public Fragmenter create(InputData inputData) throws Exception {
    	String fragmenterName = inputData.getFragmenter();
    	
        return (Fragmenter) Utilities.createAnyInstance(InputData.class, fragmenterName, inputData);
    }
}
