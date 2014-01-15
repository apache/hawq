package com.pivotal.pxf.core.format;

import com.pivotal.pxf.api.format.OneField;
import static com.pivotal.pxf.api.io.DataType.*;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.core.io.GPDBWritable;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class BridgeOutputBuilderTest
{

    private static final int UN_SUPPORTED_TYPE = -1;
    BridgeOutputBuilder builder;
    InputData input;

    @Before
    public void setUp() throws Exception
    {
        input = mock(InputData.class);
        builder = new BridgeOutputBuilder(input);
    }

    @Test
    public void testFillGPDBWritable() throws Exception
    {
        List<OneField> recFields = Arrays.asList(new OneField(INTEGER.getOID(), 0),
                new OneField(FLOAT8.getOID(), (double)0),
                new OneField(REAL.getOID(), (float)0),
                new OneField(BIGINT.getOID(), (long)0),
                new OneField(SMALLINT.getOID(), (short)0),
                new OneField(BOOLEAN.getOID(), true),
                new OneField(BYTEA.getOID(), new byte[]{0}),
                new OneField(VARCHAR.getOID(), "value"),
                new OneField(BPCHAR.getOID(), "value"),
                new OneField(TEXT.getOID(), "value"),
                new OneField(NUMERIC.getOID(), "0"),
                new OneField(TIMESTAMP.getOID(), new Timestamp(0)));
        GPDBWritable output =  builder.makeGPDBWritableOutput(recFields);
        builder.fillGPDBWritable(recFields);

        assertEquals(output.getInt(0), Integer.valueOf(0));
        assertEquals(output.getDouble(1), Double.valueOf(0));
        assertEquals(output.getFloat(2), Float.valueOf(0));
        assertEquals(output.getLong(3), Long.valueOf(0));
        assertEquals(output.getShort(4), Short.valueOf((short) 0));
        assertEquals(output.getBoolean(5), true);
        assertArrayEquals(output.getBytes(6), new byte[]{0});
        assertEquals(output.getString(7), "value\0");
        assertEquals(output.getString(8), "value\0");
        assertEquals(output.getString(9), "value\0");
        assertEquals(output.getString(10), "0\0");
        assertEquals(Timestamp.valueOf(output.getString(11)), new Timestamp(0));
    }

    @Test
    public void testFillOneGPDBWritableField() throws Exception
    {
        OneField unSupportedField = new OneField(UN_SUPPORTED_TYPE, new Date(0));
        try
        {
            builder.fillOneGPDBWritableField(unSupportedField, 0);
            fail("Unsupported data type should throw exception");
        }
        catch (UnsupportedOperationException e)
        {
            assertEquals(e.getMessage(), "Date is not supported for gpdb conversion");
        }
    }
}
