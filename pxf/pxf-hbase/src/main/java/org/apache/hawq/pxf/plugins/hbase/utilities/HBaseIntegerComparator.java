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


import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This is a Filter comparator for HBase It is external to PXF HBase code.
 * <p>
 * To use with HBase it must reside in the classpath of every region server.
 * <p>
 * It converts a value into {@link Long} before comparing.
 * The filter is good for any integer numeric comparison i.e. integer, bigint, smallint.
 * <p>
 * according to HBase 0.96 requirements, this must serialized using Protocol Buffers
 * ({@link #toByteArray()} and {@link #parseFrom(byte[])} methods).
 * <p>
 * A reference can be found in {@link SubstringComparator}.
 */
public class HBaseIntegerComparator extends ByteArrayComparable {
	private Long val;


	public HBaseIntegerComparator(Long inVal) {
		super(Bytes.toBytes(inVal));
		this.val = inVal;
	}

	/**
	 * The comparison function. Currently uses {@link Long#parseLong(String)}.
	 *
	 * @return 0 if equal;
	 *         a value less than 0 if row value is less than filter value;
	 *         and a value greater than 0 if the row value is greater than the filter value.
	 */
	@Override
	public int compareTo(byte[] value, int offset, int length) {
		/**
		 * Fix for HD-2610: query fails when recordkey is integer.
		 */
		if (length == 0)
			return 1; // empty line, can't compare.

		/**
		 * TODO optimize by parsing the bytes directly.
		 * Maybe we can even determine if it is an int or a string encoded.
		 */
		String valueAsString = new String(value, offset, length);
		Long valueAsLong = Long.parseLong(valueAsString);
		return val.compareTo(valueAsLong);
	}

	/**
	 * Returns the comparator serialized using Protocol Buffers.
	 *
	 * @return serialized comparator
	 */
	@Override
	public byte[] toByteArray() {
		ComparatorProtos.ByteArrayComparable.Builder builder = ComparatorProtos.ByteArrayComparable.newBuilder();
		builder.setValue(ByteString.copyFrom(getValue()));
		return builder.build().toByteArray();
	}

	/**
	 * Hides ("overrides") a static method in {@link ByteArrayComparable}.
	 * This method will be called in deserialization.
	 *
	 * @param pbBytes
	 *            A pb serialized instance
	 * @return An instance of {@link HBaseIntegerComparator} made from
	 *         <code>bytes</code>
	 * @throws DeserializationException if deserialization of bytes to Protocol Buffers failed
	 * @see #toByteArray
	 */
	public static ByteArrayComparable parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		ComparatorProtos.ByteArrayComparable proto;
		try {
			proto = ComparatorProtos.ByteArrayComparable.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}
		return new HBaseIntegerComparator(Bytes.toLong(proto.getValue()
				.toByteArray()));
	}
}
