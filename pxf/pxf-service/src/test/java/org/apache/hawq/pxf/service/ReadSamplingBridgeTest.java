package org.apache.hawq.pxf.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.ReadSamplingBridge;
import org.apache.hawq.pxf.service.utilities.AnalyzeUtils;
import org.apache.hawq.pxf.service.utilities.ProtocolData;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AnalyzeUtils.class, ReadSamplingBridge.class })
public class ReadSamplingBridgeTest {

    /**
     * Writable test object to test ReadSamplingBridge. The object receives a
     * string and returns it in its toString function.
     */
    public class WritableTest implements Writable {

        private String data;

        public WritableTest(String data) {
            this.data = data;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public String toString() {
            return data;
        }

    }

    private ProtocolData mockProtData;
    private ReadBridge mockBridge;
    private ReadSamplingBridge readSamplingBridge;
    private int recordsLimit = 0;
    private BitSet samplingBitSet;
    private Writable result;

    @Test
    public void getNextRecord100Percent() throws Exception {

        samplingBitSet.set(0, 100);
        recordsLimit = 100;
        when(mockProtData.getStatsSampleRatio()).thenReturn((float) 1.0);

        readSamplingBridge = new ReadSamplingBridge(mockProtData);

        result = readSamplingBridge.getNext();
        assertEquals("0", result.toString());

        result = readSamplingBridge.getNext();
        assertEquals("1", result.toString());

        when(mockBridge.getNext()).thenReturn(null);

        result = readSamplingBridge.getNext();
        assertNull(result);
    }

    @Test
    public void getNextRecord100Records10Percent() throws Exception {

        // set 10 bits from 5 to 14.
        samplingBitSet.set(5, 15);
        recordsLimit = 100;
        when(mockProtData.getStatsSampleRatio()).thenReturn((float) 0.1);

        readSamplingBridge = new ReadSamplingBridge(mockProtData);

        for (int i = 0; i < 10; i++) {
            result = readSamplingBridge.getNext();
            assertEquals("" + (i + 5), result.toString());
        }

        result = readSamplingBridge.getNext();
        assertNull(result);
    }

    @Test
    public void getNextRecord100Records90Percent() throws Exception {
        int expected = 0;

        // set the first odd numbers until 20, then all numbers until 100
        // total: 90.
        samplingBitSet.set(0, 100);
        for (int i = 0; i < 10; i++) {
            samplingBitSet.flip(i * 2);
        }
        recordsLimit = 100;
        when(mockProtData.getStatsSampleRatio()).thenReturn((float) 0.9);

        readSamplingBridge = new ReadSamplingBridge(mockProtData);

        for (int i = 0; i < 90; i++) {
            result = readSamplingBridge.getNext();
            if (i < 10) {
                expected = (i * 2) + 1;
            } else {
                expected = i + 10;
            }
            assertEquals("" + expected, result.toString());
        }
        result = readSamplingBridge.getNext();
        assertNull(result);
    }

    @Test
    public void getNextRecord350Records50Percent() throws Exception {

        // set bits 0, 40-79 (40) and 90-98 (9)
        // total 50.
        samplingBitSet.set(0);
        samplingBitSet.set(40, 80);
        samplingBitSet.set(90, 99);
        recordsLimit = 350;
        when(mockProtData.getStatsSampleRatio()).thenReturn((float) 0.5);

        readSamplingBridge = new ReadSamplingBridge(mockProtData);

        /*
         * expecting to have: 50 (out of first 100) 50 (out of second 100) 50
         * (out of third 100) 11 (out of last 50) --- 161 records
         */
        for (int i = 0; i < 161; i++) {
            result = readSamplingBridge.getNext();
            assertNotNull(result);
            if (i % 50 == 0) {
                assertEquals("" + (i * 2), result.toString());
            }
        }
        result = readSamplingBridge.getNext();
        assertNull(result);
    }

    @Test
    public void getNextRecord100000Records30Sample() throws Exception {
        int expected = 0;

        // ratio = 0.0003
        float ratio = (float) (30.0 / 100000.0);

        // set 3 records in every 10000.
        samplingBitSet.set(99);
        samplingBitSet.set(999);
        samplingBitSet.set(9999);
        recordsLimit = 100000;
        when(mockProtData.getStatsSampleRatio()).thenReturn(ratio);

        readSamplingBridge = new ReadSamplingBridge(mockProtData);

        for (int i = 0; i < 30; i++) {
            result = readSamplingBridge.getNext();
            assertNotNull(result);
            int residue = i % 3;
            int div = i / 3;
            if (residue == 0) {
                expected = 99 + (div * 10000);
            } else if (residue == 1) {
                expected = 999 + (div * 10000);
            } else {
                expected = 9999 + (div * 10000);
            }
            assertEquals("" + expected, result.toString());
        }
        result = readSamplingBridge.getNext();
        assertNull(result);
    }

    @Before
    public void setUp() throws Exception {

        mockProtData = mock(ProtocolData.class);

        mockBridge = mock(ReadBridge.class);
        PowerMockito.whenNew(ReadBridge.class).withAnyArguments().thenReturn(
                mockBridge);

        when(mockBridge.getNext()).thenAnswer(new Answer<Writable>() {
            private int count = 0;

            @Override
            public Writable answer(InvocationOnMock invocation)
                    throws Throwable {
                if (count >= recordsLimit) {
                    return null;
                }
                return new WritableTest("" + (count++));
            }
        });

        PowerMockito.mockStatic(AnalyzeUtils.class);
        samplingBitSet = new BitSet();
        when(
                AnalyzeUtils.generateSamplingBitSet(any(int.class),
                        any(int.class))).thenReturn(samplingBitSet);
    }
}
