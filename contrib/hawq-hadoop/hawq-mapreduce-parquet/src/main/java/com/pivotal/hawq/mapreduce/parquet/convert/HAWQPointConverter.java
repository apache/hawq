package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;

/**
 * group {
 *   required double x;
 *   required double y;
 * }
 * => HAWQPoint
 */
public class HAWQPointConverter extends GroupConverter {

	private ParentValueContainer parent;
	private Converter[] converters;

	private double x;
	private double y;

    public HAWQPointConverter(ParentValueContainer parent) {
		this.parent = parent;
		this.converters = new Converter[2];
		this.converters[0] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				x = value;
			}
		};
		this.converters[1] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				y = value;
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
			HAWQPoint point = new HAWQPoint(x, y);
			parent.setPoint(point);
		} catch (HAWQException e) {}
	}
}
