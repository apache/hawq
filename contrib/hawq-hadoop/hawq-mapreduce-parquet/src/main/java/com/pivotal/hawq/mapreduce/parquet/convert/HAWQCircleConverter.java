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
import com.pivotal.hawq.mapreduce.datatype.HAWQCircle;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;

/**
 * group {
 *   required double x;
 *   required double y;
 *   required double r;
 * }
 * => HAWQCircle
 */
public class HAWQCircleConverter extends GroupConverter {

	private ParentValueContainer parent;
	private Converter[] converters;

	private double x;
	private double y;
	private double r;

	public HAWQCircleConverter(ParentValueContainer parent) {
		this.parent = parent;
		this.converters = new Converter[3];
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
		this.converters[2] = new PrimitiveConverter() {
			@Override
			public void addDouble(double value) {
				r = value;
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
			HAWQCircle circle = new HAWQCircle(x, y, r);
			parent.setCircle(circle);
		} catch (HAWQException e) {}
	}
}
