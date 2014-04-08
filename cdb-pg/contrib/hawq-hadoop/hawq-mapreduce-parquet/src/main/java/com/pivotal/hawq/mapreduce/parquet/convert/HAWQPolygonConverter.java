package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.datatype.HAWQBox;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import com.pivotal.hawq.mapreduce.datatype.HAWQPolygon;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * group {
 *   required group boundbox {
 *     required double x1;
 *     required double y1;
 *     required double x2;
 *     required double y2;
 *   },
 *   repeated group points {
 *     required double x;
 *     required double y;
 *   }
 * }
 * => HAWQPolygon
 */
public class HAWQPolygonConverter extends GroupConverter {

	private ParentValueContainer parent;
	private Converter[] converters;

	private HAWQBox boundbox;
	private List<HAWQPoint> points;

	public HAWQPolygonConverter(ParentValueContainer parent) {
		this.parent = parent;
		this.converters = new Converter[2];
		this.points = new ArrayList<HAWQPoint>();

		this.converters[0] = new HAWQBoxConverter(new ParentValueContainerAdapter() {
			@Override
			public void setBox(HAWQBox x) throws HAWQException {
				HAWQPolygonConverter.this.boundbox = x;
			}
		});

		this.converters[1] = new HAWQPointConverter(new ParentValueContainerAdapter() {
			@Override
			public void setPoint(HAWQPoint x) throws HAWQException {
				HAWQPolygonConverter.this.points.add(x);
			}
		});
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return this.converters[fieldIndex];
	}

	@Override
	public void start() {
		this.boundbox = null;
		this.points.clear();
	}

	@Override
	public void end() {
		try {
			HAWQPolygon polygon = new HAWQPolygon(points, boundbox);
			parent.setPolygon(polygon);
		}  catch (HAWQException e) {}
	}
}
