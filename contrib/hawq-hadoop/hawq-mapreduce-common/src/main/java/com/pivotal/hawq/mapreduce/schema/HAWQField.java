package com.pivotal.hawq.mapreduce.schema;

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


/**
 * Represent a field in HAWQ's schema.
 */
public abstract class HAWQField {

	private boolean isOptional;
	private String name;
	private boolean isArray;

	/**
	 * Construct a HAWQField instance
	 * @param isOptional whether the field is optional
	 * @param name name of the field
	 * @param isArray whether the field is an array
	 */
	public HAWQField(boolean isOptional, String name, boolean isArray) {
		this.isOptional = isOptional;
		this.name = name;
		this.isArray = isArray;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof HAWQField))
			return false;
		return equalsField((HAWQField) obj);
	}

	/**
	 * Test equality of two HAWQField objects.
	 * @param other
	 * @return true if the two HAWQField are equal, false otherwise
	 */
	protected abstract boolean equalsField(HAWQField other);

	/**
	 * indicate whether the field is of primitive type
	 * @return true if the field is of primitive type, false otherwise
	 */
	abstract public boolean isPrimitive();

	/**
	 * Write field's String representation to a buffer `sb` with specified indent
	 * @param sb buffer to use
	 * @param indent any indentation to use
	 */
	abstract public void writeToStringBuilder(StringBuilder sb, String indent);

	/**
	 * Cast the object into HAWQPrimitiveField.
	 * @return the casted object
	 * @throws ClassCastException throw an ClassCastException when the cast fails.
	 */
	public HAWQPrimitiveField asPrimitive() throws ClassCastException {
		if (!isPrimitive()) {
			throw new ClassCastException(this + " is not a primitive field");
		}
		return (HAWQPrimitiveField) this;
	}

	/**
	 * Cast the object into HAWQGroupField.
	 * @return the casted object
	 * @throws ClassCastException throw an ClassCastException when the cast fails.
	 */
	public HAWQGroupField asGroup() throws ClassCastException {
		if (isPrimitive()) {
			throw new ClassCastException(this + " is not a group field");
		}
		return (HAWQGroupField) this;
	}

	/**
	 * Get whether the field is optional.
	 * @return true if optional, false if not
	 */
	public boolean isOptional() {
		return isOptional;
	}

	/**
	 * Get the field's name
	 * @return field's name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Get whether the field is an array.
	 * @return true if is array, false if not
	 */
	public boolean isArray() {
		return isArray;
	}
}
