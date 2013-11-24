package com.pxf.infra.utils.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/***
 * Utility for working with regular expressions
 */
public abstract class RegexUtils {

	/**
	 * Match pattern on data
	 * 
	 * @param pattern
	 * @param data
	 * @return
	 */
	public static boolean match(String pattern, String data) {

		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(data);

		return m.find();
	}
}
