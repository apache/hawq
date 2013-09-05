package com.pivotal.hawq.mapreduce.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Map;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.*;
import java.lang.String;
import java.sql.ResultSet;

/**
 *   InputFormat result class
 *   */
public class HAWQInputFormatPrepareData
{
    //public static int[] columnNum = {1, 2, 4, 8, 16, 32, 64, 128};
    //public static int[] columnNum = {1, 2, 128, 1600};
    public static int[] columnNum = {1, 2, 128};
    public Hashtable readTypeDef(String defFilePath)
    {
        try{
            FileReader fr = new FileReader(defFilePath);
            BufferedReader br = new BufferedReader(fr);
            Hashtable<String,ArrayList> dataset = new Hashtable<String, ArrayList>();
            while(br.ready())
            {
                String strline = br.readLine();
                String[] sArray = strline.split("  ");
                ArrayList oneList = new ArrayList<String>();
                for(int i=1;i<sArray.length;i++)
                    if(!sArray[i].equals("#"))
                        oneList.add(sArray[i]);
                dataset.put(sArray[0],oneList);
            }

            //local verify code
            //ArrayList oneArray = dataset.get("money");
            //Iterator it1 = oneArray.iterator();
            //while(it1.hasNext()){System.out.println(it1.next());}
            br.close();
            fr.close();
            return dataset;
        }catch (Exception e) {
              e.printStackTrace();
              return null;
        }
    }
    public Hashtable readTypeClassification(String defFilePath)
    {
        try{
            FileReader fr = new FileReader(defFilePath);
            BufferedReader br = new BufferedReader(fr);
            Hashtable<Integer, ArrayList> dataset = new Hashtable<Integer, ArrayList>();
            int cnt = 0;
            while(br.ready())
            {
                String strline = br.readLine();
                String[] sArray = strline.split(",");
                ArrayList oneList = new ArrayList<String>();
                for(int i=0;i<sArray.length;i++)
                       oneList.add(sArray[i]);
                dataset.put(cnt,oneList);
                cnt++;
            }
            br.close();
            fr.close();
            return dataset;
        }catch (Exception e) {
              e.printStackTrace();
              return null;
        }
    }

    public boolean prepareSqlForSingleType(Hashtable<String,ArrayList> dataset ,String type_name)
    {
         try{
             Random r = new Random();
             HAWQJDBCCommon jdbc = new HAWQJDBCCommon();
 
             for(int i=0; i<columnNum.length; i++)
             {
                 String casename = type_name + "_" + Integer.toString(columnNum[i])+ "column";
                 String filename = casename.replace("(","");
                 filename = filename.replace(")","");
                 filename = filename.replace("[]","array");
                 OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream("src/test/java/com/pivotal/hawq/mapreduce/query/" + filename + ".sql"),"UTF-8");
                 //generate drop table if exist statement
                 String dropSt = "drop table if exists " + filename;
                 out.write(dropSt+"\n");
                 //generate sql for create table statement
                 String createSt = "create table " + filename + " (";
                 for(int j=0; j<columnNum[i]-1; j++)
                     createSt = createSt + "c" + Integer.toString(j) + " " + type_name + ",";
                 createSt = createSt + "c" + Integer.toString(columnNum[i]-1) + " " + type_name + ")";
                 //add AO Table Attribute
                 int ran = r.nextInt(5);
                 if(ran == 0)
                     createSt = createSt + " WITH (APPENDONLY=true, ORIENTATION=ROW, CHECKSUM=TRUE)";
                 else if(ran == 1)
                     createSt = createSt + " WITH (APPENDONLY=true, ORIENTATION=ROW, BLOCKSIZE=8192)";
                 else if(ran == 2)
                 {
                     createSt = createSt + "WITH (APPENDONLY=true, ORIENTATION=ROW, " ;
                     int compressType=r.nextInt(2);
                     if(compressType==0)
                     {
                         createSt = createSt + "COMPRESSTYPE=ZLIB, ";
                         createSt = createSt + "COMPRESSLEVEL=" + Integer.toString(r.nextInt(9)+1) + ")";
                     }
                     else createSt = createSt + "COMPRESSTYPE=QUICKLZ,COMPRESSLEVEL=1)";
                     //System.out.println(createSt);
                 } 
                 createSt = createSt + ";";
                 //System.out.println(createSt); 
                 out.write(createSt+"\n");
                 //generate sql for several insert statements
                 //int insertNum = r.nextInt(10);
                 //should be above sentence to cover empty table. 
                 int insertNum = r.nextInt(9)+1;
                 for(int iter=0; iter<insertNum; iter++)
                 {
                     String insertSt = "insert into " + filename + " values (";
                     for(int j=0; j<columnNum[i]-1; j++)
                         insertSt = insertSt + getSimpleValue(dataset, type_name, r) + ", ";
                      insertSt = insertSt + getSimpleValue(dataset, type_name, r) +");";
                      //System.out.println(insertSt);
                      out.write(insertSt+"\n");
                 }
                 out.flush();
                 out.close();
                
                 jdbc.runsqlfile("src/test/java/com/pivotal/hawq/mapreduce/query/" + filename + ".sql");
                 //generate anaswer file based on HAWQ output.
                 jdbc.generateAnsFile(filename, "src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".ans"); 
             }
             return true; 
          }catch (Exception e) {
             e.printStackTrace();
             return false;
          }
    }

    public String tpchload(String scale, String tabletype, String orient, String haspartition)
    {
        try{
                HAWQJDBCCommon jdbc = new HAWQJDBCCommon();
                int segs_num = 0;
                String sql = "SELECT COUNT(*) FROM gp_segment_configuration WHERE content>=0;";
                ResultSet rs = jdbc.runsql(sql);
                if (rs.next()) {
                    segs_num = rs.getInt("count");
                }

                String cmd = String.format("./generate_load_tpch.pl -scale %s -num %d -port %s -db %s -table %s -orient %s -partition %s -dbversion hawq -compress false", scale, segs_num, jdbc.getMasterPort(), "gptest", tabletype, orient, haspartition);
                Process process = Runtime.getRuntime().exec(cmd);
                process.waitFor();
                System.out.println("statement execute success : Tpch loading data completed !");

        }catch (Exception e) {
            e.printStackTrace();
        }
        String tablesuffix = "_" + tabletype + "_" + orient;
        return tablesuffix;
    }

    public String prepareSqlForMultipleType(Hashtable<String,ArrayList> dataset, Hashtable<Integer,ArrayList> dataclassification)
    {
        try{
               
             Random r = new Random();
             HAWQJDBCCommon jdbc = new HAWQJDBCCommon();

             ArrayList colnames = new ArrayList<String>();  
             for (int i=0; i<dataclassification.size(); i++)
             {
                  int cols = 1;
                  ArrayList types = dataclassification.get(i);
                  if (types.size() > 1)
                       cols = r.nextInt(2) + 1;
                  for(int j=0; j<cols; j++) 
                  {
                       int ran = r.nextInt(types.size());
                       String curType = types.get(ran).toString();
                       colnames.add(curType);
                  }
             }

            String filename = "multipletype";
            for(int i = 0; i < colnames.size(); i++)
            {
                filename += "_" + colnames.get(i).toString().trim();
            }
            filename = filename.replace("(","");
            filename = filename.replace(")","");
            filename = filename.replace("[]","array");
            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream("src/test/java/com/pivotal/hawq/mapreduce/query/" + filename + ".sql"),"UTF-8");
           
            //generate drop table if exist statement
            String dropSt = "drop table if exists " + filename;
            out.write(dropSt+"\n");
            //generate sql for create table statement
            String createSt = "create table " + filename + " (";
            for(int j=0; j<colnames.size()-1; j++)
                createSt = createSt + "c" + Integer.toString(j) + " " + colnames.get(j).toString().trim() + ",";
            createSt = createSt + "c" + Integer.toString(colnames.size()-1) + " " + colnames.get(colnames.size()-1).toString().trim() + ")";

            //add AO Table Attribute
            int ran = r.nextInt(5);
            if(ran == 0)
                createSt = createSt + " WITH (APPENDONLY=true, ORIENTATION=ROW, CHECKSUM=TRUE)";
            else if(ran == 1)
                createSt = createSt + " WITH (APPENDONLY=true, ORIENTATION=ROW, BLOCKSIZE=8192)";
            else if(ran == 2)
            {
                createSt = createSt + "WITH (APPENDONLY=true, ORIENTATION=ROW, " ;
                int compressType=r.nextInt(2);
                if(compressType==0)
                {
                    createSt = createSt + "COMPRESSTYPE=ZLIB, ";
                    createSt = createSt + "COMPRESSLEVEL=" + Integer.toString(r.nextInt(9)+1) + ")";
                }
                else createSt = createSt + "COMPRESSTYPE=QUICKLZ,COMPRESSLEVEL=1)";
                //System.out.println(createSt);
            } 
            createSt = createSt + ";";
            //System.out.println(createSt); 
            out.write(createSt+"\n");

            //generate sql for several insert statements
            //int insertNum = r.nextInt(10);
            //should be above sentence to cover empty table. 
            int insertNum = r.nextInt(9)+1;
            for(int iter=0; iter<insertNum; iter++)
            {
                String insertSt = "insert into " + filename + " values (";
                for(int j=0; j<colnames.size()-1; j++)
                    insertSt = insertSt + getSimpleValue(dataset, colnames.get(j).toString().trim(), r) + ", ";
                 insertSt = insertSt + getSimpleValue(dataset, colnames.get(colnames.size()-1).toString().trim(), r) +");";
                 //System.out.println(insertSt);
                 out.write(insertSt+"\n");
            }
            out.flush();
            out.close();
                
            jdbc.runsqlfile("src/test/java/com/pivotal/hawq/mapreduce/query/" + filename + ".sql");
            //generate anaswer file based on HAWQ output.
            jdbc.generateAnsFile(filename, "src/test/java/com/pivotal/hawq/mapreduce/query/"+filename+".ans"); 
            return filename; 
          }catch (Exception e) {
             e.printStackTrace();
             return null;
          }
    }
    private String getSimpleValue(Hashtable<String,ArrayList> dataset, String type_name, Random r)
    {
        try{
            ArrayList valueList = dataset.get(type_name);
            int pos = r.nextInt(valueList.size());
            return valueList.get(pos).toString();
        }catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

}
