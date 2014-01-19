package com.pivotal.pxf.api;

/*
 * Defines one field on a deserialized record. 
 * 'type' is in OID values recognized by GPDBWritable
 * 'val' is the actual field value
 */
public class OneField
{
    public OneField() {}
    
    public OneField(int type, Object val)
    {
        this.type = type;
        this.val = val;
    }
    
	public int type;
	public Object val;
}
