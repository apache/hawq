package com.pivotal.pxf.format;

/*
 * Represents one row in the external system data store. Supports 
 * the general case where one row contains both a record and a 
 * separate key like in the HDFS key/value model for MapReduce 
 * (Example: HDFS sequence file)
 */
public class OneRow
{
	private Object key;
	private Object data;
	
	/*
	 * Default constructor
	 */
	public OneRow()
	{
		key = null;
		data = null;
	}
	
	/*
	 * Constructor sets key and data
	 */
	public OneRow(Object inKey, Object inData)
	{
		key = inKey;
		data = inData;
	}
	
	/*
	 * Copy constructor 
	 */	
	public OneRow(OneRow copy)
	{
		this.key = copy.key;
		this.data = copy.data;
	}
	
	/*
	 * Setter for key
	 */
	public void setKey(Object inKey)
	{
		key = inKey;
	}

	/*
	 * Setter for data
	 */	
	public void setData(Object inData)
	{
		data = inData;
	}

	/*
	 * Accessor for key
	 */		
	public Object getKey()
	{
		return key;
	}

	/*
	 * Accessor for data
	 */		
	public Object getData()
	{
		return data;
	}
    
    /*
     * Show content
     */
    public String toString()
    {
        String content = new String("OneRow:");
        if (key != null)
        {
            content += key.toString();
        }
        else
        {
            content += "null";
        }
        content += "->";
        if (data != null)
        {
            content += data.toString();
        }
        else
        {
            content += "null";
        }
        return content;
    }
}

