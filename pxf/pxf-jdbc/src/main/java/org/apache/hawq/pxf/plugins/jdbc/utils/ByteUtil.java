package org.apache.hawq.pxf.plugins.jdbc.utils;

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

import org.apache.commons.lang.ArrayUtils;

/**
 * A tool class for byte array merging, splitting and conversion
 */
public class ByteUtil {
    public static byte[] mergeBytes(byte[] b1, byte[] b2) {
        return ArrayUtils.addAll(b1,b2);
    }

    /**
     * Split a byte[] array into two arrays, each of which represents a value of type long
     */
    public static byte[][] splitBytes(byte[] bytes) {
        final int N = 8;
        int len = bytes.length / N;
        byte[][] newBytes = new byte[len][];
        int j = 0;
        for (int i = 0; i < len; i++) {
            newBytes[i] = new byte[N];
            for (int k = 0; k < N; k++) newBytes[i][k] = bytes[j++];
        }
        return newBytes;
    }

    /**
     * Convert a value of type long to a byte[] array
     */
    public static byte[] getBytes(long value) {
        byte[] b = new byte[8];
        b[0] = (byte) ((value >> 56) & 0xFF);
        b[1] = (byte) ((value >> 48) & 0xFF);
        b[2] = (byte) ((value >> 40) & 0xFF);
        b[3] = (byte) ((value >> 32) & 0xFF);
        b[4] = (byte) ((value >> 24) & 0xFF);
        b[5] = (byte) ((value >> 16) & 0xFF);
        b[6] = (byte) ((value >> 8) & 0xFF);
        b[7] = (byte) ((value >> 0) & 0xFF);
        return b;
    }

    /**
     * Convert a byte[] array to a value of type long
     */
    public static long toLong(byte[] b) {
        return ((((long) b[7]) & 0xFF) +
                ((((long) b[6]) & 0xFF) << 8) +
                ((((long) b[5]) & 0xFF) << 16) +
                ((((long) b[4]) & 0xFF) << 24) +
                ((((long) b[3]) & 0xFF) << 32) +
                ((((long) b[2]) & 0xFF) << 40) +
                ((((long) b[1]) & 0xFF) << 48) +
                ((((long) b[0]) & 0xFF) << 56));
    }
}
