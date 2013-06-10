package com.pivotal.pxf.format;

/*
 * Defines one field ion a deserialized record the type is in OID values recognized by GPDBWritable
 * and val is the actual field value
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
