package com.pivotal.pxf.api.exception;

/*
 * FragmenterUserDataException - an exception available for 
 * raising 'userdata' parsing issues and unexpected differences
 * between userdata producer (fragmenter) and consumers (accessor/resolver).
 */
public class FragmenterUserDataException extends Exception
{
	public FragmenterUserDataException() { }
	
    public FragmenterUserDataException(Throwable cause) { super(cause); }
    
    public FragmenterUserDataException(String message) { super(message); }
	
    public FragmenterUserDataException(String message, Throwable cause) { super(message, cause); }
}
