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

public class HAWQAOInputFormatFeatureTest_SingleType{

    static HAWQInputFormatPrepareData prepareData;
    static HAWQJDBCCommon jdbc;
    static Hashtable<String,ArrayList> dataset;
    static HAWQInputFormatResult result;

    @BeforeClass
    public static void setUp() {
        prepareData = new HAWQInputFormatPrepareData();
        jdbc = new HAWQJDBCCommon();
        result = new HAWQInputFormatResult();
        dataset = prepareData.readTypeDef("src/test/java/com/pivotal/hawq/mapreduce/query/dataset");
    }

    @AfterClass
    public static void tearDown() {
    }

    @Ignore
    public void testSingleTypeInt2(){
        doTest("int2", true);
    }

    @Ignore
    public void testSingleTypeInt4(){
        doTest("int4", true);
    }
    
    @Ignore
    public void testSingleTypeInt8(){
        doTest("int8", true);
    }
    
    @Ignore
    public void testSingleTypeBool(){
        doTest("bool", true);
    }
    
    @Test
    public void testSingleTypeFloat4(){
        doTest("float4", true);
    }

    @Test
    public void testSingleTypeFloat8(){
        doTest("float8", true);
    }

    @Ignore
    public void testSingleTypeNumeric(){
        doTest("numeric", true);
    }

    @Test
    public void testSingleTypePoint(){
        doTest("point", true);
    }

    @Ignore
    public void testSingleTypeVarchar(){
        doTest("varchar(10)", true);
    }

    @Ignore
    public void testSingleTypeChar(){
        doTest("char(10)", true);
    }

    @Ignore
    public void testSingleTypeTime(){
        doTest("time", true);
    }
    
    @Ignore
    public void testSingleTypeDate(){
        doTest("date", true);
    }

    @Ignore
    public void testSingleTypeInterval(){
        doTest("interval", true);
    }

    @Ignore
    public void testSingleTypeTimeStamp(){
        doTest("timestamp", true);
    }
 
    @Ignore
    public void testSingleTypeBytea(){
        doTest("bytea", true);
    }

    @Ignore
    public void testSingleTypeCircle(){
        doTest("circle", true);
    }

    @Ignore
    public void testSingleTypePath(){
        doTest("path", true);
    }
    
    @Ignore
    public void testSingleTypeArray(){
        doTest("int4[]", true);
    }

    @Ignore
    public void testSingleTypeBit(){
        doTest("bit", true);
    }

    @Ignore
    public void testSingleTypevarBit(){
        doTest("varbit", true);
    }

    @Ignore
    public void testSingleTypebit5(){
        doTest("bit(5)", true);
    }

    @Ignore
    public void testSingleTypeMacAddr(){
        doTest("macaddr", true);
    }

    //Common method for SingleType test
    private void doTest(String type_name, boolean checkresult){
        try{
           HAWQInputFormatCommon inputFormat = new HAWQInputFormatCommon();

           prepareData.prepareSqlForSingleType(dataset, type_name);
           
           for(int i=0; i<prepareData.columnNum.length; i++)
           {
               String casename = type_name + "_" +  Integer.toString(prepareData.columnNum[i])+ "column";
               String filename = casename.replace("(","");
               filename = filename.replace(")","");
               filename = filename.replace("[]","array");
               System.out.println("Executing test case: " + casename);
               inputFormat.runMapReduce(casename, jdbc.getMasterAddress()+":"+jdbc.getMasterPort()+"/gptest", filename, "src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".out");
               Thread.sleep(1000);
               jdbc.droptable(filename);
               if(checkresult)
                   if(!result.checkResult("src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".ans", "src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".out", "sortcheck"))
                       Assert.fail("TEST FAILURE: The answer file and out file is different: " + filename);
           }
        }catch(Exception e){
            e.printStackTrace();
            //clean up
            for(int i=0; i<prepareData.columnNum.length; i++)
            {
               String casename = type_name + "_" + Integer.toString(prepareData.columnNum[i])+ "column";
               String filename = casename.replace("(","");
               filename = filename.replace(")","");
               filename = filename.replace("[]","array");
               jdbc.droptable(filename);
            }
        }
    }
}

