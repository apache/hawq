package org.apache.hawq.pxf.service.io;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
