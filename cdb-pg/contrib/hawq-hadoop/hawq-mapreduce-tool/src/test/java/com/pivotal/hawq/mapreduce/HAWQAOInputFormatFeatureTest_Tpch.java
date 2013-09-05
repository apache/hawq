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
import java.util.Map;

public class HAWQAOInputFormatFeatureTest_Tpch{

    static HAWQInputFormatPrepareData prepareData;
    static HAWQJDBCCommon jdbc;
    static HAWQInputFormatResult result;
    static HAWQInputFormatCommon inputformat;
    static String dbname = "gptest";
    static String username = null;
    static String password = null;

    @Before
    public void setUp() {
        prepareData = new HAWQInputFormatPrepareData();
        jdbc = new HAWQJDBCCommon();
        result = new HAWQInputFormatResult();
        inputformat = new HAWQInputFormatCommon();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testTpchAOTable(){
        doTest("0.001", "ao", "row", "false", "false");
    }

    @Test
    public void testTpchAOPartitionTable(){
        doTest("0.001", "ao", "row", "true", "false");
    }

    @Test
    public void testTpchAOTableUseMetadata(){
        doTest("0.001", "ao", "row", "false", "true");
    }

    @Test
    public void testTpchAOPartitionTableUseMetadata(){
        doTest("0.001", "ao", "row", "true", "true");
    }

    private void doTest(String loadsize, String tabletype, String orient, String haspartition, String frommd){
        try{

           String tablesuffix = prepareData.tpchload(loadsize, tabletype, orient, haspartition);
           
           String tablename = "lineitem" + tablesuffix;
           String casename = tablename;
           if (haspartition == "true"){
               casename += "_partition";
           }
           if (frommd == "true"){
               casename += "_metadata";
           }

           String sqlans = "src/test/java/com/pivotal/hawq/mapreduce/query/tpch_" + casename + ".ans";
           String sqlout = "src/test/java/com/pivotal/hawq/mapreduce/query/tpch_" + casename + ".out";
           jdbc.generateAnsFile(tablename, sqlans);

           String dburl = jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/" + dbname; 
           if (frommd == "true"){
                String metadata_path = "src/test/java/com/pivotal/hawq/mapreduce/query/metadata_" + dbname + "_" + tablename;
                inputformat.extractMetadata(dbname, jdbc.getMasterPort(), tablename, metadata_path);
                inputformat.fetchFromMetadata(casename, metadata_path, sqlout);
           }else
           { 
                inputformat.runMapReduce(casename, dburl, tablename, sqlout);
           }
           Thread.sleep(1000);

           if(!result.checkResult(sqlans, sqlout, "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: " + casename);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
