package org.apache.hawq.pxf.plugins.s3;

import org.apache.hawq.pxf.api.OneField;

public class NullableOneField extends OneField {

	public NullableOneField(int type, Object val) {
		super(type, val);
	}

	public NullableOneField() {
		super();
	}

	/**
	 * The purpose of this class is to handle the case where <code>val</code> is
	 * null so, rather than calling <code>toString()</code> on null, it will simply
	 * return null.
	 */
	public String toString() {
		String rv = null;
		if (null != this.val) {
			rv = this.val.toString();
		}
		return rv;
	}

}
