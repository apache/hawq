package com.pivotal.hawq.mapreduce.util;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.*;
import java.lang.String;

/**
 *   InputFormat result class
 *   */
public class HAWQInputFormatResult
{
    public boolean checkResult(String answerfile, String outfile, String checkmethod)
    {
         ArrayList ansList = new ArrayList();
         ArrayList outList = new ArrayList();
         try{
              FileReader fr_ans = new FileReader(answerfile);
              BufferedReader br_ans = new BufferedReader(fr_ans);
              while(br_ans.ready())
              {
                  String s = br_ans.readLine();
                  ansList.add(s);
              }
              br_ans.close();
              fr_ans.close();

              FileReader fr_out = new FileReader(outfile);
              BufferedReader br_out = new BufferedReader(fr_out);
              while(br_out.ready())
              {
                  String s = br_out.readLine();
                  outList.add(s);
              }
              br_out.close();
              fr_out.close();

              if(checkmethod.equals("sortcheck"))
              {
                  Collections.sort(ansList);
                  Collections.sort(outList);
                  return compareArrayList(ansList, outList, answerfile);
              }
              
              //To be added: other check result method.
              return true;
              
          }catch (Exception e) {
              e.printStackTrace();
              return false;
          }
    }
   
    public boolean compareArrayList(ArrayList ansList, ArrayList outList, String answerfile)
    {
        int outlen=outList.size();
        int anslen=ansList.size();
        if(outlen != anslen)
        {
            System.out.println("The length of answer file and out file is different, please check...");
            return false;
        }
        for(int i=0; i<outlen; i++)
        {
            String s1 = outList.get(i).toString();
            String s2 = ansList.get(i).toString();
            
            if(answerfile.indexOf("bool_")!=-1)
            {
                s1 = s1.replace("true","t");
                s1 = s1.replace("false","f");
                if(!s1.equals(s2))
                {
                    System.out.println("The answer file and out file is different, please check...");
                    return false;
                }
            }
            if((answerfile.indexOf("float")!=-1)||(answerfile.indexOf("double")!=-1))
            {
                String[] Array1=s1.split("\\|");
                String[] Array2=s2.split("\\|");
                int length = Array1.length;
                for(int j=0; j<length; j++)
                {
                    if((Array1[j].equals(""))||(Array1[j].equals("|")))
                        continue;
                    if(Array1[j].equals(Array2[j]))
                        continue;
                    double d1 = Double.parseDouble(Array1[j]);
                    double d2 = Double.parseDouble(Array2[j]);
                    if((((d1-d2)/d1>0.0001))||((d1-d2)/d1<-0.0001))
                    {
			 System.out.println(s1);
			 System.out.println(s2);
                   	 System.out.println("The answer file and out file is different, please check...");
                   	 return false;
   	            }
                }
            }
            else if(!s1.equals(s2))
            {
                System.out.println("The answer file and out file is different, please check...");
                return false;
            }
        }
        return true;
    }
}
