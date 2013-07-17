package com.pivotal.pxf.format;

/*
 * Defines one field on a deserialized record. 
 * 'type' is in OID values recognized by GPDBWritable
 * 'val' is the actual field value
 */
public class OneField
{
    public OneField() {}
    
    public OneField(int Type, Object Val)
    {
        type = Type;
        val = Val;
    }
    
	public int type;
	public Object val;
}
