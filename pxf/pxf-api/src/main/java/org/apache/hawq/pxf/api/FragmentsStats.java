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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * FragmentsStats holds statistics for a given path.
 */
public class FragmentsStats {

    /**
     * Default fragment size. Assuming a fragment is equivalent to a block in
     * HDFS, we guess a full fragment size is 64MB.
     */
    public static final long DEFAULT_FRAGMENT_SIZE = 67108864L;

    private static final Log LOG = LogFactory.getLog(FragmentsStats.class);

    // number of fragments
    private long fragmentsNumber;
    // first fragment size
    private SizeAndUnit firstFragmentSize;
    // total fragments size
    private SizeAndUnit totalSize;

    /**
     * Enum to represent unit (Bytes/KB/MB/GB/TB)
     */
    public enum SizeUnit {
        /**
         * Byte
         */
        B,
        /**
         * KB
         */
        KB,
        /**
         * MB
         */
        MB,
        /**
         * GB
         */
        GB,
        /**
         * TB
         */
        TB;
    };

    /**
     * Container for size and unit
     */
    public class SizeAndUnit {
        long size;
        SizeUnit unit;

        /**
         * Default constructor.
         */
        public SizeAndUnit() {
            this.size = 0;
            this.unit = SizeUnit.B;
        }

        /**
         * Constructor.
         *
         * @param size size
         * @param unit unit
         */
        public SizeAndUnit(long size, SizeUnit unit) {
            this.size = size;
            this.unit = unit;
        }

        /**
         * Returns size.
         *
         * @return size
         */
        public long getSize() {
            return this.size;
        }

        /**
         * Returns unit (Byte/KB/MB/etc.).
         *
         * @return unit
         */
        public SizeUnit getUnit() {
            return this.unit;
        }

        @Override
        public String toString() {
            return size + "" + unit;
        }
    }

    /**
     * Constructs an FragmentsStats.
     *
     * @param fragmentsNumber number of fragments
     * @param firstFragmentSize first fragment size (in bytes)
     * @param totalSize total size (in bytes)
     */
    public FragmentsStats(long fragmentsNumber, long firstFragmentSize,
                          long totalSize) {
        this.setFragmentsNumber(fragmentsNumber);
        this.setFirstFragmentSize(firstFragmentSize);
        this.setTotalSize(totalSize);
    }

    /**
     * Given a {@link FragmentsStats}, serialize it in JSON to be used as the
     * result string for HAWQ. An example result is as follows:
     * <code>{"PXFFragmentsStats":{"fragmentsNumber":3,"firstFragmentSize":{"size"=67108864,"unit":"B"},"totalSize":{"size"=200000000,"unit"="B"}}}</code>
     *
     * @param stats the data to be serialized
     * @return the result in json format
     * @throws IOException if converting to JSON format failed
     */
    public static String dataToJSON(FragmentsStats stats) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // mapper serializes all members of the class by default
        return "{\"PXFFragmentsStats\":" + mapper.writeValueAsString(stats)
                + "}";
    }

    /**
     * Given a stats structure, convert it to be readable. Intended for
     * debugging purposes only.
     *
     * @param stats the data to be stringify
     * @param datapath the data path part of the original URI (e.g., table name,
     *            *.csv, etc.)
     * @return the stringified data
     */
    public static String dataToString(FragmentsStats stats, String datapath) {
        return "Statistics information for \"" + datapath + "\" "
                + " Number of Fragments: " + stats.fragmentsNumber
                + ", first Fragment size: " + stats.firstFragmentSize
                + ", total size: " + stats.totalSize;
    }

    /**
     * Returns number of fragments for a given data source.
     *
     * @return number of fragments
     */
    public long getFragmentsNumber() {
        return fragmentsNumber;
    }

    private void setFragmentsNumber(long fragmentsNumber) {
        this.fragmentsNumber = fragmentsNumber;
    }

    /**
     * Returns the size in bytes of the first fragment.
     *
     * @return first fragment size (in byte)
     */
    public SizeAndUnit getFirstFragmentSize() {
        return firstFragmentSize;
    }

    private void setFirstFragmentSize(long firstFragmentSize) {
        this.firstFragmentSize = setSizeAndUnit(firstFragmentSize);
    }

    /**
     * Returns the total size of a given source. Usually it means the
     * aggregation of all its fragments size.
     *
     * @return total size
     */
    public SizeAndUnit getTotalSize() {
        return totalSize;
    }

    private void setTotalSize(long totalSize) {
        this.totalSize = setSizeAndUnit(totalSize);
    }

    private SizeAndUnit setSizeAndUnit(long originalSize) {
        final long THRESHOLD = Integer.MAX_VALUE / 2;
        int orderOfMagnitude = 0;
        SizeAndUnit sizeAndUnit = new SizeAndUnit();
        sizeAndUnit.size = originalSize;

        while (sizeAndUnit.size > THRESHOLD) {
            sizeAndUnit.size /= 1024;
            orderOfMagnitude++;
        }

        sizeAndUnit.unit = getSizeUnit(orderOfMagnitude);
        return sizeAndUnit;
    }

    private SizeUnit getSizeUnit(int orderOfMagnitude) {
        SizeUnit unit;
        switch (orderOfMagnitude) {
            case 0:
                unit = SizeUnit.B;
                break;
            case 1:
                unit = SizeUnit.KB;
                break;
            case 2:
                unit = SizeUnit.MB;
                break;
            case 3:
                unit = SizeUnit.GB;
                break;
            case 4:
                unit = SizeUnit.TB;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported order of magnitude "
                                + orderOfMagnitude
                                + ". Size's order of magnitue can be a value between 0(Bytes) and 4(TB)");
        }
        return unit;
    }
}
