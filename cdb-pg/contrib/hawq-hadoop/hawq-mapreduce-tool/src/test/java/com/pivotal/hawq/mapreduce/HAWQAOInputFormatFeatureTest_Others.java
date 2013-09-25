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

public class HAWQAOInputFormatFeatureTest_Others{

    static HAWQInputFormatPrepareData prepareData;
    static HAWQJDBCCommon jdbc;
    static Hashtable<String,ArrayList> dataset;
    static HAWQInputFormatResult result;
    static String queryRoot;

    static class insertThread extends Thread
    {
        int threadId;
        boolean checkresult;
        private static List<Thread> runningThreads = new ArrayList<Thread>();
        public insertThread(int i) {threadId = i;}
        public void run()
        {
            regist(this);
            System.out.println("thread started " + threadId);
            jdbc.runsqlfile("src/test/java/com/pivotal/hawq/mapreduce/query/MultiWirte" + Integer.toString(threadId) + ".sql");
            unRegist(this);
            System.out.println("thread finished " + threadId);
        }
        public void regist(Thread t)
        {
            synchronized(runningThreads)
            {
                runningThreads.add(t);
            }
        }
        public void unRegist(Thread t)
        {
            synchronized (runningThreads)
            {
                runningThreads.remove(t);
            }
        }
        public static boolean hasThreadRunning(){
            return (runningThreads.size()>0);
        }
    }

    @BeforeClass
    public static void setUp() {
	System.out.println("Executing test suite: Others");
        prepareData = new HAWQInputFormatPrepareData();
        jdbc = new HAWQJDBCCommon();
        result = new HAWQInputFormatResult();
        queryRoot = "src/test/java/com/pivotal/hawq/mapreduce/query/";
    }

    @AfterClass
    public static void tearDown() {
    }


    @Test
    public void testDiffSchema(){
        try{

           System.out.println("Executing test case: same table under different schema");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForDiffSchema.sql");

           jdbc.generateAnsFile("schematest",queryRoot + "schematest_default.ans");
           jdbc.generateAnsFile("schema1.schematest",queryRoot + "schematest_1.ans");
           jdbc.generateAnsFile("schema2.schematest",queryRoot + "schematest_2.ans");

           inputFormat.runMapReduce("schematest", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "schematest", queryRoot + "schematest_default.out");
           inputFormat.runMapReduce("schema1.schematest", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "schema1.schematest", queryRoot + "schematest_1.out");
           inputFormat.runMapReduce("schema2.schematest", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "schema2.schematest", queryRoot + "schematest_2.out");

           if(!result.checkResult(queryRoot + "schematest_default.ans", queryRoot + "schematest_default.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: schematest in default schema");
           if(!result.checkResult(queryRoot + "schematest_1.ans", queryRoot + "schematest_1.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: schema1.schematest");
           if(!result.checkResult(queryRoot + "schematest_2.ans", queryRoot + "schematest_2.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: schema2.schematest");
           
           jdbc.runsqlfile(queryRoot + "cleanupForDiffSchema.sql");
           System.out.println("Successfully finish test case: same table under different schema");
        
        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForDiffSchema.sql");
        }
    }

   @Test
   public void testEmptyTable(){
        try{
           System.out.println("Executing test case: empty table");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForEmptyTable.sql");
           jdbc.generateAnsFile("emptytable",queryRoot + "emptytable.ans");
           inputFormat.runMapReduce("emptytable", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "emptytable", queryRoot + "emptytable.out");
           if(!result.checkResult(queryRoot + "emptytable.ans", queryRoot + "emptytable.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: emptytable");
           
           jdbc.runsqlfile(queryRoot + "cleanupForEmptyTable.sql");
           System.out.println("Successfully finish test case: empty table");

        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForEmptyTable.sql");
        }
   }
   
   @Test
   public void testEmptyTableFromMetadata(){
        try{
           System.out.println("Executing test case: empty table for metadata");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForEmptyTable.sql");
           jdbc.generateAnsFile("emptytable",queryRoot + "emptytable.ans");
   
           String dbname = "gptest";
           String tablename = "emptytable"; 
           String metadata_path = queryRoot + "metadata_" + dbname + "_" + tablename;
           String sqlout = queryRoot + "emptytable.out";
           inputFormat.extractMetadata(dbname, jdbc.getMasterPort(), tablename, metadata_path);
           inputFormat.fetchFromMetadata("emptytable", metadata_path, sqlout);
           if(!result.checkResult(queryRoot + "emptytable.ans", queryRoot + "emptytable.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: emptytable");
           
           jdbc.runsqlfile(queryRoot + "cleanupForEmptyTable.sql");
           System.out.println("Successfully finish test case: empty table for metadata");
        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForEmptyTable.sql");
        }
   }

   @Ignore
   public void testLargeTuple(){
        try{
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
        }catch(Exception e){
            e.printStackTrace();
        }
   }
   
   @Test
   public void testAddPartition(){
        try{
	   System.out.println("Executing test case: add partition");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForAddPartition.sql");
           jdbc.generateAnsFile("addpartition",queryRoot + "addpartition.ans");
           inputFormat.runMapReduce("addpartition", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "addpartition", queryRoot + "addpartition.out");
           if(!result.checkResult(queryRoot + "addpartition.ans", queryRoot + "addpartition.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: addpartition");
           
           jdbc.runsqlfile(queryRoot + "cleanupForAddPartition.sql");
	   System.out.println("Successfully finish test case: add partition");
        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForAddPartition.sql");
        }
   }
   @Test
   public void testCOTable(){
        try{
           System.out.println("Executing test case: CO table");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForCOTable.sql");
           inputFormat.runMapReduce("cotable", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "cotable", queryRoot + "cotable.out");
           jdbc.runsqlfile(queryRoot + "cleanupForCOTable.sql");
           Assert.fail("No exception for cotable thrown.");
        }catch(Exception e){
            assertTrue(e instanceof HAWQException);
            assertTrue(e.getMessage().contains("Only append only(row orientation) table is supported"));
            jdbc.runsqlfile(queryRoot + "cleanupForCOTable.sql");
	    System.out.println("Successfully finish test case: CO table");
        }
   }
   
   @Test
   public void testInvalidMetadata(){
        try{
           System.out.println("Executing test case: invalid metadata");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForInvalidMetadata.sql");
   
           String dbname = "gptest";
           String tablename = "invalidmetadata"; 
           String metadata_path = queryRoot + "metadata_" + dbname + "_" + tablename;
           String sqlout = queryRoot + "invalidmetadata.out";
           inputFormat.extractMetadata(dbname, jdbc.getMasterPort(), tablename, metadata_path);
           OutputStreamWriter metadata_fh = new OutputStreamWriter(new FileOutputStream(metadata_path),"UTF-8");
           metadata_fh.write("Test|For|Invalid|metadata\n");
           metadata_fh.flush();
           metadata_fh.close();
           inputFormat.fetchFromMetadata("invalidmetadata", metadata_path, sqlout);

           jdbc.runsqlfile(queryRoot + "cleanupForInvalidMetadata.sql");
           Assert.fail("No exception for invalidmetadata thrown.");
        }catch(Exception e){
            assertTrue(e instanceof HAWQException);
            assertTrue(e.getMessage().contains("This is not a YAML style file"));
            jdbc.runsqlfile(queryRoot + "cleanupForInvalidMetadata.sql");
	    System.out.println("Successfully finish test case: invalid metadata");
        }
   }

    @Test
    public void testMultiWrite(){
        try{

	   System.out.println("Executing test case: multi write using hawq then run MRInputFormat");
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();
           jdbc.runsqlfile(queryRoot + "setupForMultiWirte.sql");
 
           for(int i=0; i<5; i++)
           {
               insertThread insert = new insertThread(i);
               insert.start();
               Thread.sleep(200);
           }

           // wait for all insert finish before next step
           while(true)
           {
               if(!insertThread.hasThreadRunning())
                   break;
               Thread.sleep(1000);
           }

           jdbc.generateAnsFile("multiwritetable", queryRoot + "multiwrite.ans");
 
           inputFormat.runMapReduce("multiwritetable", jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", "multiwritetable", queryRoot + "multiwrite.out");
           Thread.sleep(1000);
           if(!result.checkResult(queryRoot + "multiwrite.ans", queryRoot + "multiwrite.out", "sortcheck"))
               Assert.fail("TEST FAILURE: The answer file and out file is different: multiwritetable");

           System.out.println("Successfully finish test case: multi write using hawq then run MRInputFormat"); 
           jdbc.runsqlfile(queryRoot + "cleanupForMultiWirte.sql");
        }catch(Exception e){
            e.printStackTrace();
            jdbc.runsqlfile(queryRoot + "cleanupForMultiWirte.sql");
        }
    }
}
