package org.apache.hawq.pxf.service;

import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.service.utilities.Utilities;

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
