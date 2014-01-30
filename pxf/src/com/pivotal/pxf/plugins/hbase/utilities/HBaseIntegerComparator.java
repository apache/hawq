package com.pivotal.pxf.plugins.hbase.utilities;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This is a Filter comparator for HBase It is external to PXF HBase code
 * 
 * To use with HBase it must reside in the classpath of every region server
 * 
 * It converts a value into Long before comparing The filter is good for any
 * integer numeric comparison i.e. integer, bigint, smallint
 * 
 * according to HBase 0.96 requirements, this must serialized using pb
 * (toByteArray and parseFrom methods)
 * 
 * A reference can be found in {@link SubstringComparator}
 */
public class HBaseIntegerComparator extends ByteArrayComparable {
	private Long val;

	public HBaseIntegerComparator(Long inVal) {
		super(Bytes.toBytes(inVal));
		this.val = inVal;
	}

	/**
	 * The comparison function currently is using Long.parseLong
	 */
	@Override
	public int compareTo(byte[] value, int offset, int length) {
		/*
		 * Fix for HD-2610: query fails when recordkey is integer.
		 */
		if (length == 0)
			return 1; // empty line, can't compare.

		// TODO optimize by parsing the bytes directly.
		// Maybe we can even determine if it is an int or a string encoded
		String valueAsString = new String(value, offset, length);
		Long valueAsLong = Long.parseLong(valueAsString);
		return val.compareTo(valueAsLong);
	}

	/**
	 * @return The comparator serialized using pb
	 */
	@Override
	public byte[] toByteArray() {
		ComparatorProtos.ByteArrayComparable.Builder builder = ComparatorProtos.ByteArrayComparable.newBuilder();
		builder.setValue(ByteString.copyFrom(getValue()));
		return builder.build().toByteArray();
	}

	/**
	 * "Override" a static method in ByteArrayComparable. In the Deserialization
	 * this method will be call.
	 * 
	 * @param pbBytes
	 *            A pb serialized instance
	 * @return An instance of {@link HBaseIntegerComparator} made from
	 *         <code>bytes</code>
	 * @throws DeserializationException
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
