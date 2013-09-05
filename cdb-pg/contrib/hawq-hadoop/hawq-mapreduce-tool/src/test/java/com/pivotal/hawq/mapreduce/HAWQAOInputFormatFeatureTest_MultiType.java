package com.pivotal.hawq.mapreduce;

import java.io.*;
import java.lang.*;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import com.pivotal.hawq.mapreduce.util.HAWQInputFormatCommon;
import com.pivotal.hawq.mapreduce.util.HAWQInputFormatResult;
import com.pivotal.hawq.mapreduce.util.HAWQJDBCCommon;
import com.pivotal.hawq.mapreduce.util.HAWQInputFormatPrepareData;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class HAWQAOInputFormatFeatureTest_MultiType{

    static HAWQInputFormatPrepareData prepareData;
    static HAWQJDBCCommon jdbc;
    static Hashtable<String,ArrayList> dataset;
    static HAWQInputFormatResult result;
    static Hashtable<Integer,ArrayList> dataclassfication;

    @BeforeClass
    public static void setUp() {
        prepareData = new HAWQInputFormatPrepareData();
        jdbc = new HAWQJDBCCommon();
        result = new HAWQInputFormatResult();
        dataset = prepareData.readTypeDef("src/test/java/com/pivotal/hawq/mapreduce/query/dataset");
        dataclassfication = prepareData.readTypeClassification("src/test/java/com/pivotal/hawq/mapreduce/query/dataclassification");
    }

    @AfterClass
    public static void tearDown() {
    }

    @Test
    public void testMultipleType1(){
        doTest(true);
    }
    
    @Test
    public void testMultipleType2(){
        doTest(true);
    }

    @Test
    public void testMultipleType3(){
        doTest(true);
    }

    //Common method for MultipleType test
    private void doTest(boolean checkresult){
        String filename = null;
        try{
           filename = prepareData.prepareSqlForMultipleType(dataset, dataclassfication);
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           System.out.println("Executing test case: " + filename);
           inputFormat.runMapReduce(filename, jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", filename, "src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".out");
           Thread.sleep(1000);
           jdbc.droptable(filename);
           if(checkresult)
               if(!result.checkResult("src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".ans", "src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".out", "sortcheck"))
                   Assert.fail("TEST FAILURE: The answer file and out file is different: " + filename);
        }catch(Exception e){
            e.printStackTrace();
            //clean up
            if (filename != null)
                jdbc.droptable(filename);
            }
    }
}

