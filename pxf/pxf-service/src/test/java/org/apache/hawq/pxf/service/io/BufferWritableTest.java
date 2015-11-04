package org.apache.hawq.pxf.service.io;

import static org.junit.Assert.*;

import org.junit.Test;

public class BufferWritableTest {

    @Test
    public void append() throws Exception {
        String data1 = "פרק ראשון ובו יסופר יסופר";
        String data2 = "פרק שני ובו יסופר יסופר";

        BufferWritable bw1 = new BufferWritable(data1.getBytes());

        assertArrayEquals(data1.getBytes(), bw1.buf);

        bw1.append(data2.getBytes());

        assertArrayEquals((data1+data2).getBytes(), bw1.buf);
    }
}
