package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.datatype.HAWQPath;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * group {
 *   required boolean is_open;
 *   repeated group {
 *     required double x;
 *     required double y;
 *   }
 * }
 * => HAWQPath
 */
public class HAWQPathConverter extends GroupConverter {

	private ParentValueContainer parent;
	private Converter[] converters;

	private boolean isOpen;
	private List<HAWQPoint> points;

	public HAWQPathConverter(ParentValueContainer parent) {
		this.parent = parent;
		this.points = new ArrayList<HAWQPoint>();
		this.converters = new Converter[2];

		this.converters[0] = new HAWQRecordConverter.HAWQPrimitiveConverter(new ParentValueContainerAdapter() {
			@Override
			public void setBoolean(boolean x) throws HAWQException {
				HAWQPathConverter.this.isOpen = x;
			}
		});
		this.converters[1] = new HAWQPointConverter(new ParentValueContainerAdapter() {
			@Override
			public void setPoint(HAWQPoint x) throws HAWQException {
				HAWQPathConverter.this.points.add(x);
			}
		});
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return this.converters[fieldIndex];
	}

	@Override
	public void start() {
		points.clear();
	}

	@Override
	public void end() {
		try {
			HAWQPath path = new HAWQPath(isOpen, points);
			parent.setPath(path);
		} catch (HAWQException e) {}
	}
}
