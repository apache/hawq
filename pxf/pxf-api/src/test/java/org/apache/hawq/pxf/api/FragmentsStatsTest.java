package org.apache.hawq.pxf.api;

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

import java.io.IOException;

import org.apache.hawq.pxf.api.FragmentsStats.SizeUnit;
import org.junit.Test;

public class FragmentsStatsTest {

    @Test
    public void ctorSizeByte() {
        ctorSizeTest(10, 100, 100, SizeUnit.B, 1000000, 1000000, SizeUnit.B);
    }

    @Test
    public void ctorSizeKB() {
        ctorSizeTest(40, 50, 50, SizeUnit.B, (long) Math.pow(2, 32), (long) Math.pow(2, 22),
                SizeUnit.KB);
    }

    @Test
    public void ctorSizeMB() {
        ctorSizeTest(20, 50, 50, SizeUnit.B, (long) Math.pow(2, 40), (long) Math.pow(2, 20),
                SizeUnit.MB);
    }

    @Test
    public void ctorSizeGB() {
        ctorSizeTest(25, 1000000, 1000000, SizeUnit.B, (long) Math.pow(6, 20),
                (long) Math.pow(6, 20) / (long) Math.pow(2, 30), SizeUnit.GB);
    }

    @Test
    public void ctorSizeTB() {
        ctorSizeTest(25, 20000000, 20000000, SizeUnit.B, (long) Math.pow(5, 30),
                (long) Math.pow(5, 30) / (long) Math.pow(2, 40), SizeUnit.TB);
    }

    @Test
    public void ctorSize0() {
        ctorSizeTest(0, 0, 0, SizeUnit.B, 0, 0, SizeUnit.B);
    }

    @Test
    public void dataToJSON() throws IOException {
        FragmentsStats fragmentsStats = new FragmentsStats(25, 20000000, (long) Math.pow(5, 30));
        String json = FragmentsStats.dataToJSON(fragmentsStats);
        String expectedJson = "{\"PXFFragmentsStats\":" +
                "{\"fragmentsNumber\":" + fragmentsStats.getFragmentsNumber() +
                ",\"firstFragmentSize\":" +
                "{\"size\":" + fragmentsStats.getFirstFragmentSize().getSize() +
                ",\"unit\":\"" + fragmentsStats.getFirstFragmentSize().getUnit() + "\"}" +
                ",\"totalSize\":" +
                "{\"size\":" + fragmentsStats.getTotalSize().getSize() +
                ",\"unit\":\"" + fragmentsStats.getTotalSize().getUnit() + "\"}" +
                "}}";
        assertEquals(expectedJson, json);
    }

    @Test
    public void dataToString() {
        FragmentsStats fragmentsStats = new FragmentsStats(25, 2000000000, (long) Math.pow(5, 30));
        String path = "la la la";
        String str = FragmentsStats.dataToString(fragmentsStats, path);
        String expected =  "Statistics information for \"" + path + "\" "
                + " Number of Fragments: " + 25
                + ", first Fragment size: " + 1953125 + "KB"
                + ", total size: " + 8388607 + "TB";
        assertEquals(expected, str);
    }

    private void ctorSizeTest(long fragsNum, long firstFragSize,
                              long expectedFirstFragSize,
                              SizeUnit expectedFirstFragSizeUnit, long totalSize,
                              long expectedTotalSize,
                              SizeUnit expectedTotalSizeUnit) {
        FragmentsStats fragmentsStats = new FragmentsStats(fragsNum,
                firstFragSize, totalSize);
        assertEquals(fragsNum, fragmentsStats.getFragmentsNumber());
        assertEquals(expectedFirstFragSize,
                fragmentsStats.getFirstFragmentSize().size);
        assertEquals(expectedFirstFragSizeUnit,
                fragmentsStats.getFirstFragmentSize().unit);
        assertEquals(expectedTotalSize, fragmentsStats.getTotalSize().size);
        assertEquals(expectedTotalSizeUnit,
                fragmentsStats.getTotalSize().unit);
    }
}
