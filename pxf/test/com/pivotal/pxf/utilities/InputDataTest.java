package com.pivotal.pxf.utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.pivotal.pxf.format.OutputFormat;

@RunWith(JUnit4.class)
public class InputDataTest 
{
	Map<String, String> parameters;

    @Test
    public void inputDataCreated() 
	{
		InputData input = new InputData(parameters);

		assertEquals(System.getProperty("greenplum.alignment"), "all");
		assertEquals(input.totalSegments(), 2);
		assertEquals(input.segmentId(), -44);
		assertEquals(input.outputFormat(), OutputFormat.FORMAT_TEXT);
		assertEquals(input.serverName(), "my://bags");
		assertEquals(input.serverPort(), -8020);
		assertFalse(input.hasFilter());
		assertNull(input.filterString());
		assertEquals(input.columns(), 0);
		assertEquals(input.dataFragmentsSize(), 0);
		assertEquals(input.getDataFragments(), new ArrayList<Integer>());
		assertNull(input.getRecordkeyColumn());
		assertEquals(input.accessor(), "are");
		assertEquals(input.resolver(), "packed");
		assertNull(input.GetAvroFileSchema());
		assertEquals(input.tableName(), "i'm/ready/to/go");
		assertEquals(input.path(), "/i'm/ready/to/go");
		assertEquals(input.getUserProperty("i'm-standing-here"), "outside-your-door");
		assertEquals(input.getParametersMap(), parameters);
    }

	@Test
	public void inputDataCopied()
	{
		InputData input = new InputData(parameters);
		InputData copy = new InputData(input);
		assertEquals(copy.getParametersMap(), input.getParametersMap());
	}

	/*
	 * setUp function called before each test
	 */
	@Before
	public void setUp()
	{
		parameters = new HashMap<String, String>();

		parameters.put("X-GP-ALIGNMENT", "all");
		parameters.put("X-GP-SEGMENT-ID", "-44");
		parameters.put("X-GP-SEGMENT-COUNT", "2");
		parameters.put("X-GP-HAS-FILTER", "0");
		parameters.put("X-GP-FORMAT", "TEXT");
		parameters.put("X-GP-URL-HOST", "my://bags");
		parameters.put("X-GP-URL-PORT", "-8020");
		parameters.put("X-GP-ATTRS", "-1");
		parameters.put("X-GP-ACCESSOR", "are");
		parameters.put("X-GP-RESOLVER", "packed");
		parameters.put("X-GP-DATA-DIR", "i'm/ready/to/go");
		parameters.put("X-GP-I'M-STANDING-HERE", "outside-your-door");
	}
}
