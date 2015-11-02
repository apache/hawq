package org.apache.hawq.pxf.service;

import org.apache.hawq.pxf.api.Analyzer;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.service.utilities.Utilities;

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
