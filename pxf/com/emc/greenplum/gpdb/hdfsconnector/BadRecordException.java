package com.emc.greenplum.gpdb.hdfsconnector;

/*
 * Exception class that repesents any problem that occured while fetching or parsing
 * a record from the user's HDFS input data file. Any place in the GP Bridge code that
 * encounters such a data problem will throw this exception. There is one place in the 
 * bridge code that catches the exception, and reacts by issuing a GPDBWritable record
 * with the errorFlag set. - This catch logic is located in BasicBridge.GetNext()
 */
public class BadRecordException extends Exception 
{
}
