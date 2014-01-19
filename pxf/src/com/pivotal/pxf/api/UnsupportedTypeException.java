package com.pivotal.pxf.api;

/*
 * Exception when the resolver serializes/deserializes an unsupported type.
 */
public class UnsupportedTypeException extends RuntimeException
{

	public UnsupportedTypeException() {}
    
    public UnsupportedTypeException(Throwable cause) { super(cause); }
    
    public UnsupportedTypeException(String message) { super(message); }
}
