package com.pivotal.hawq.mapreduce;

import java.io.*;
import java.lang.*;
import org.junit.*;
import static org.junit.Assert.*;
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
import java.util.List;

public class HAWQAOInputFormatFeatureTest_BFV{

    static HAWQInputFormatPrepareData prepareData;
    static HAWQJDBCCommon jdbc;
    static Hashtable<String,ArrayList> dataset;
    static HAWQInputFormatResult result;
    static String queryRoot;

    @BeforeClass
    public static void setUp() {
	System.out.println("Executing test suite: BFV");
        prepareData = new HAWQInputFormatPrepareData();
        jdbc = new HAWQJDBCCommon();
        result = new HAWQInputFormatResult();
        queryRoot = "src/test/java/com/pivotal/hawq/mapreduce/query/";
    }

    @AfterClass
    public static void tearDown() {
    }

   // GPSQL-1488
   @Test
   public void testGetBoolean(){
        try{
           System.out.println("Executing test case: testGetBoolean");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForGetBoolean.sql");
           jdbc.generateAnsFile("getboolean",queryRoot + "bool_getboolean.ans");
           inputFormat.runMapReduce("getboolean", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "getboolean", queryRoot + "bool_getboolean.out");
           if(!result.checkResult(queryRoot + "bool_getboolean.ans", queryRoot + "bool_getboolean.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: getboolean");
           
           jdbc.runsqlfile(queryRoot + "cleanupForGetBoolean.sql");
           System.out.println("Successfully finish test case: testGetBoolean");

        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForGetBoolean.sql");
        }
   }

   // GPSQL-1491
   @Test
   public void testTruncateTable(){
        try{
           System.out.println("Executing test case: truncate table");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForTruncateTable.sql");
           jdbc.generateAnsFile("truncateTable",queryRoot + "truncateTable.ans");
           inputFormat.runMapReduce("truncateTable", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "truncateTable", queryRoot + "truncateTable.out");
           if(!result.checkResult(queryRoot + "truncateTable.ans", queryRoot + "truncateTable.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: truncateTable");
           
           jdbc.runsqlfile(queryRoot + "cleanupForTruncateTable.sql");
           System.out.println("Successfully finish test case: truncate table");

        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForTruncateTable.sql");
        }
   }

   // GPSQL-1489   
   @Test
   public void testTruncateTableFromMetadata(){
        try{
           System.out.println("Executing test case: truncate table for metadata");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForTruncateTable.sql");
           jdbc.generateAnsFile("truncateTable",queryRoot + "truncateTableForMetadata.ans");
   
           String dbname = "gptest";
           String tablename = "truncatetable"; 
           String metadata_path = queryRoot + "metadata_" + dbname + "_" + tablename;
           String sqlout = queryRoot + "truncateTableForMetadata.out";
           inputFormat.extractMetadata(dbname, jdbc.getMasterPort(), tablename, metadata_path);
           inputFormat.fetchFromMetadata("truncatetable", metadata_path, sqlout);
           if(!result.checkResult(queryRoot + "truncateTableForMetadata.ans", queryRoot + "truncateTableForMetadata.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: truncate table for metadata");
           
           jdbc.runsqlfile(queryRoot + "cleanupForTruncateTable.sql");
           System.out.println("Successfully finish test case: truncate table for metadata");
        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForTruncateTable.sql");
        }
   }
}
