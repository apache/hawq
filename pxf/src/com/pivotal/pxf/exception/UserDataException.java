package com.pivotal.pxf.exception;

/*
 * UserDataException - an exception available for raising 'userdata'
 * parsing issues and unexpected differences between userdata producer
 * (fragmenter) and consumers (accessor/resolver).
 */
public class UserDataException extends Exception
{
	public UserDataException() { }
	
    public UserDataException(Throwable cause) { super(cause); }
    
    public UserDataException(String message) { super(message); }
	
    public UserDataException(String message, Throwable cause) { super(message, cause); }
}
