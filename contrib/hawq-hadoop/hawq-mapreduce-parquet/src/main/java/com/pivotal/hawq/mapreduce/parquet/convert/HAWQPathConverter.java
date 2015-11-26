package com.pivotal.hawq.mapreduce.parquet.convert;

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
