package com.pivotal.hawq.mapreduce.util;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.sql.DriverManager;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.*;

/**
 *   JDBC common class
 *   */
public class HAWQJDBCCommon
{
    static String username = "gpadmin";
    static String password = "changeme";
    static String connurl = "jdbc:postgresql://localhost:5432/gptest";

    public HAWQJDBCCommon(){
        
        String port = getMasterPort();
        if(port.length()!=0)
            connurl = connurl.replace("5432",port);
        String address = getMasterAddress();
        if(address.length()!=0)
            connurl = connurl.replace("localhost",address);
    }

    public ResultSet runsql(String sql)
    {
         ResultSet rs = null;
         try{
              Connection conn = (Connection) DriverManager.getConnection(connurl, username, password);
              if(!conn.isClosed())
              {
                  conn.setAutoCommit(false);
                  Statement stmt = conn.createStatement();
                  if (null == stmt) {
                      System.out.println("connection create statement fail");
                      System.exit(1);
                  }

                  rs = stmt.executeQuery(sql);

                  conn.commit();
                  conn.close();
              }
          }catch (Exception e) {
              e.printStackTrace();
          }
       return rs;
    }

    public void runsqlfile(String filepath)
    {
         try{
              Connection conn = (Connection) DriverManager.getConnection(connurl, username, password);
              if(!conn.isClosed())
              {
                  //System.out.println("connect success :" + connurl);
                  conn.setAutoCommit(false);
                  Statement stmt = conn.createStatement();
                  if (null == stmt) {
                      System.out.println("connection create statement fail");
                      System.exit(1);
                  }

                  FileReader fr = new FileReader(filepath);
                  BufferedReader br = new BufferedReader(fr);

                  while(br.ready())
                  {
                      String sql = br.readLine();
                      stmt.execute(sql);
                      //System.out.println("Statement execute success : " + sql);
                  }
                  br.close();
                  fr.close();
                  conn.commit();
                  conn.close();
              }
          }catch (Exception e) {
              e.printStackTrace();
          }
    }
 
    public boolean droptable(String tablename)
    {
        try{
            Connection conn = (Connection) DriverManager.getConnection(connurl, username, password);
            if(!conn.isClosed())
            {
                Statement stmt = conn.createStatement();
                if (null == stmt) {
                    System.out.println("connection create statement fail");
                    return false;
                }
                //System.out.println("connection create statement success");

                String sql = "DROP TABLE IF EXISTS " + tablename;
                stmt.execute(sql);
                System.out.println("statement execute success : " + sql);

                stmt.close();
                conn.close();
            } 
        }catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
 
    public boolean generateAnsFile(String tablename, String ans_path)
    {
        try{
            Connection conn = (Connection) DriverManager.getConnection(connurl, username, password);
            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(ans_path),"UTF-8");
            if(!conn.isClosed())
            {
                Statement stmt = conn.createStatement();
                if (null == stmt) {
                    System.out.println("connection create statement fail");
                    return false;
                }
                //System.out.println("connection create statement success");
                String sql = "SELECT * FROM  " + tablename;
                ResultSet rs = stmt.executeQuery(sql);
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    for(int i=1; i<columnCount; i++)
                        out.write(rs.getString(i)+"|");
                    out.write(rs.getString(columnCount)+"\n");
                }

                stmt.close();
                conn.close();
                out.flush();
                out.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String getMasterPort()
    {
        String port = "5432";
        try{
            Map m = System.getenv();
            if(m.containsKey("PG_BASE_PORT"))
            {
                port = m.get("PG_BASE_PORT").toString();
                return port;
            }
            return port;
        }catch (Exception e) {
            e.printStackTrace();
            return port;
        }
    }

    public String getMasterAddress()
    {
        String address = "localhost";
        try{
            Map m = System.getenv();
            if(m.containsKey("PG_BASE_ADDRESS"))
            {
                address = m.get("PG_BASE_ADDRESS").toString();
                return address;
            }
            return address;
        }catch (Exception e) {
            e.printStackTrace();
            return address;
        }
    }
}
