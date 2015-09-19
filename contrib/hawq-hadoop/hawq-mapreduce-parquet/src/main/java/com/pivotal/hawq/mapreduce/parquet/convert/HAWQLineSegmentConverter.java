package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.datatype.HAWQLseg;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;

/**
 * group {
 *   required double x1;
 *   required double y1;
 *   required double x2;
 *   required double y2;
 * }
 * => HAWQLseg
 */
public class HAWQLineSegmentConverter extends GroupConverter {

	private ParentValueContainer parent;
	private Converter[] converters;

	private double x1;
	private double y1;
	private double x2;
	private double y2;

	public HAWQLineSegmentConverter(ParentValueContainer parent) {
		this.parent = parent;
		this.converters = new Converter[4];
		this.converters[0] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				x1 = value;
			}
		};
		this.converters[1] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				y1 = value;
			}
		};
		this.converters[2] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				x2 = value;
			}
		};
		this.converters[3] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				y2 = value;
			}
		};
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return this.converters[fieldIndex];
	}

	@Override
	public void start() {}

	@Override
	public void end() {
		try {
			HAWQLseg lseg = new HAWQLseg(x1, y1, x2, y2);
			parent.setLseg(lseg);
		} catch (HAWQException e) {}
	}
}
