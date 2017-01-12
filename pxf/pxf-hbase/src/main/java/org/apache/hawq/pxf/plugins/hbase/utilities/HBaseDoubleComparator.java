package org.apache.hawq.pxf.plugins.hbase.utilities;

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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDoubleComparator extends ByteArrayComparable{

    private Double val;

    public HBaseDoubleComparator(Double inVal) {
        super(Bytes.toBytes(inVal));
        this.val = inVal;
    }

    @Override
    public byte[] toByteArray() {
        ComparatorProtos.ByteArrayComparable.Builder builder = ComparatorProtos.ByteArrayComparable.newBuilder();
        builder.setValue(ByteString.copyFrom(getValue()));
        return builder.build().toByteArray();
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
        if (length == 0)
            return 1;

        String valueAsString = new String(value, offset, length);
        Double valueAsDouble = Double.parseDouble(valueAsString);
        return val.compareTo(valueAsDouble);
    }

    public static ByteArrayComparable parseFrom(final byte[] pbBytes) throws DeserializationException {
        ComparatorProtos.ByteArrayComparable proto;
        try {
            proto = ComparatorProtos.ByteArrayComparable.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        return new HBaseDoubleComparator(Bytes.toDouble(proto.getValue().toByteArray()));
    }
}

