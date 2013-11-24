package com.pxf.infra.utils.jsystem.sut;

import org.w3c.dom.DOMException;

import jsystem.framework.sut.Sut;

/**
 * Utilities for working and parsing SUT
 */
public abstract class SutUtils {

	/**
	 * Get value from SUT file
	 * 
	 * @param sut
	 * @param xpath
	 * @return
	 * @throws DOMException
	 * @throws Exception
	 */
	public static String getValue(Sut sut, String xpath) throws DOMException,
			Exception {

		return sut.getAllValues(xpath).get(0).getTextContent();
	}
}
