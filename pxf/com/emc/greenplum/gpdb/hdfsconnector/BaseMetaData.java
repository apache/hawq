package com.emc.greenplum.gpdb.hdfsconnector;

import java.util.Map;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/*
 * Common configuration of all MetaData classes
 * Provides read-only access to common parameters supplied using system properties
 */


public class BaseMetaData
{
	protected Map<String, String> requestParametersMap;
	private   Log Log;
	
	/*
	 * When a property is not found we throw an exception from getProperty method(). The exception message
	 * has a generic form containg the HTTP option name. For example:
	 * --  Property "X-GP-ACCESSOR" has no value in current request  --
	 * X-GP-ACCESSOR is a GPFUSION internal term, and it would be better not to display it to the user.
	 * With propertyErrorMap we make possible to attach a specific explanatory message to a property
	 * that will be used instead of the generic one.
	 */
	protected Map<String, String> propertyErrorMap = new HashMap<String, String>();
	
    /* Constructor of HDMetaData
     * Parses greenplum.* configuration variables
     */
	public BaseMetaData(Map<String, String> paramsMap)
	{
		Log = LogFactory.getLog(BaseMetaData.class);
		requestParametersMap = paramsMap;

	}

    /* Copy contructor of BaseMetaData
     * Used to create from an extending class
     */
    public BaseMetaData(BaseMetaData copy)
    {
		Log = LogFactory.getLog(BaseMetaData.class);
		
		this.requestParametersMap = copy.requestParametersMap;
		this.propertyErrorMap     = copy.propertyErrorMap;
    }

	/* 
     * Returns a property as a string type
     */	
    protected String getProperty(String property)
    {
        String result	= requestParametersMap.get(property);
		String error	= new String("");
		
        if (result == null)
        {
			if (propertyErrorMap.containsKey(property))
				error = (String)propertyErrorMap.get(property);
			else 
			{
				error = "Internal server error. Property \"" + property + 
						"\" has no value in current request";
				Log.error(error);
			}
            throw new IllegalArgumentException(error);
        }

        return result;
    }
	
	/* 
     * Unlike getProperty(), it will not fail if the property is not found. It will just return null instead
     */	
    protected String getOptionalProperty(String property)
    {
        return requestParametersMap.get(property);
    }

	/*
	 * Returns a property as an int type
	 */
	protected int getIntProperty(String property)
	{
		return Integer.parseInt(getProperty(property));
	}

	/*
	 * Returns a property as boolean type
	 *
	 * A boolean property is defined as an int where 0 means false 
	 * and anything else true (like C)
	 */
	protected boolean getBoolProperty(String property)
	{
		return getIntProperty(property) != 0;
	}
}
