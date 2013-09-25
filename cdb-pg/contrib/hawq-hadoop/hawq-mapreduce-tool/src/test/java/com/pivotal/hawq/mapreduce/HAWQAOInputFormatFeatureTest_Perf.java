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

public class HAWQAOInputFormatFeatureTest_Perf{

    static HAWQInputFormatPrepareData prepareData;
    static HAWQJDBCCommon jdbc;
    static HAWQInputFormatResult result;
    static HAWQInputFormatCommon inputformat;
    static String dbname = "gpsqltest";
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
        doTest(" lineitem_ao_row_quicklz_level1", "false", "false");
    }

    @Test
    public void testTpchAOPartitionTable(){
        doTest("lineitem_ao_row_quicklz_level1_part", "true", "false");
    }

    private void doTest(String tablename, String haspartition, String frommd){
        try{
           long start = System.currentTimeMillis();
           String dburl = jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/" + dbname;
           String sqlout = "src/test/java/com/pivotal/hawq/mapreduce/query/" + tablename + "_perf.out"; 
           if (frommd == "true"){
                String metadata_path = "src/test/java/com/pivotal/hawq/mapreduce/query/metadata_" + dbname + "_" + tablename;
                inputformat.extractMetadata(dbname, jdbc.getMasterPort(), tablename, metadata_path);
                inputformat.fetchFromMetadata("perf_test", metadata_path, sqlout);
           }else
           {
                inputformat.runMapReduce("perf_test", dburl, tablename, sqlout, true);
           }
	   long end = System.currentTimeMillis();
           
           System.out.println("Total time use: " + (end - start)/1000 + " seconds");
           Thread.sleep(1000);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
