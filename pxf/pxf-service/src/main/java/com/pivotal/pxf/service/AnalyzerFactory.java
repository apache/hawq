package com.pivotal.pxf.service;

import com.pivotal.pxf.api.Analyzer;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.service.utilities.Utilities;

/*
 * Factory class for creation of Analyzer objects. The actual Analyzer object is "hidden" behind
 * an Analyzer abstract class which is returned by the AnalyzerFactory. 
 */
public class AnalyzerFactory {
    static public Analyzer create(InputData inputData) throws Exception {
    	String analyzerName = inputData.getAnalyzer();
    	
        return (Analyzer) Utilities.createAnyInstance(InputData.class, analyzerName, inputData);
    }
}
