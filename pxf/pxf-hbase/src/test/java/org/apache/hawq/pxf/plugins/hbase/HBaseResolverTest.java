package org.apache.hawq.pxf.plugins.hbase;

import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseResolver.class})
public class HBaseResolverTest {
    InputData inputData;
    HBaseTupleDescription tupleDesc;

    @Test
    /*
	 * Test construction of HBaseResolver.
	 * 
	 * HBaseResolver is created and then HBaseTupleDescription 
	 * creation is verified
	 */
    public void construction() throws Exception {
        inputData = mock(InputData.class);
        tupleDesc = mock(HBaseTupleDescription.class);
        PowerMockito.whenNew(HBaseTupleDescription.class).withArguments(inputData).thenReturn(tupleDesc);

        HBaseResolver resolver = new HBaseResolver(inputData);
        PowerMockito.verifyNew(HBaseTupleDescription.class).withArguments(inputData);
    }

    @Test
	/*
	 * Test the convertToJavaObject method
	 */
    public void testConvertToJavaObject() throws Exception {
        Object result;

        inputData = mock(InputData.class);
        tupleDesc = mock(HBaseTupleDescription.class);
        PowerMockito.whenNew(HBaseTupleDescription.class).withArguments(inputData).thenReturn(tupleDesc);

        HBaseResolver resolver = new HBaseResolver(inputData);

		/*
		 * Supported type, No value.
		 * Should successfully return Null.
		 */
        result = resolver.convertToJavaObject(20, "bigint", null);
        assertNull(result);
		
		/*
		 * Supported type, With value
		 * Should successfully return a Java Object that holds original value
		 */
        result = resolver.convertToJavaObject(20, "bigint", "1234".getBytes());
        assertEquals(((Long) result).longValue(), 1234L);
		
		/*
		 * Supported type, Invalid value
		 * Should throw a BadRecordException, with detailed explanation.
		 */
        try {
            result = resolver.convertToJavaObject(20, "bigint", "not_a_numeral".getBytes());
            fail("Supported type, Invalid value should throw an exception");
        } catch (BadRecordException e) {
            assertEquals("Error converting value 'not_a_numeral' to type bigint. (original error: For input string: \"not_a_numeral\")", e.getMessage());
        } catch (Exception e) {
            fail("Supported type, Invalid value expected to catch a BadRecordException, caught Exception");
        }
		
		/*
		 * Unsupported type
		 * Should throw an Exception, indicating the name of the unsupported type
		 */
        try {
            result = resolver.convertToJavaObject(600, "point", "[1,1]".getBytes());
            fail("Unsupported data type should throw exception");
        } catch (Exception e) {
            assertEquals("Unsupported data type point", e.getMessage());
        }

    }
}
