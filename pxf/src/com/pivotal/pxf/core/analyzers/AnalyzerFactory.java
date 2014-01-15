package com.pivotal.pxf.core.analyzers;

import com.pivotal.pxf.api.analyzers.Analyzer;
import com.pivotal.pxf.core.utilities.Utilities;
import com.pivotal.pxf.api.utilities.InputData;

/*
 * Factory class for creation of Analyzer objects. The actual Analyzer object is "hidden" behind
 * an Analyzer abstract class which is returned by the AnalyzerFactory. 
 */
public class AnalyzerFactory {
    static public Analyzer create(InputData conf) throws Exception {
        String analyzerName = conf.getProperty("X-GP-ANALYZER");
        return (Analyzer) Utilities.createAnyInstance(InputData.class, analyzerName, conf);
    }
}
