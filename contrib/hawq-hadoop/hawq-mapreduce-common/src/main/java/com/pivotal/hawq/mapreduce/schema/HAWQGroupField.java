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


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represent a group field in HAWQ's schema. A group field contains one or more sub fields.
 * Note that with the group, sub field's index starts with 1 rather than 0.
 */
public class HAWQGroupField extends HAWQField {

	private List<HAWQField> fields;
	private String dataTypeName; /* group maps to UDT in HAWQ, thus this is the name of the UDT */

	private Map<String, Integer> indexByName;

	/**
	 * Constructor of HAWQGroupField.
	 *
	 * <p>Instead of using this constructor, we recommend you to use
	 * factory methods defined in HAWQSchema:</p>
	 *
	 * <ul>
	 *     <li>HAWQSchema.required_group(...)</li>
	 *     <li>HAWQSchema.optional_group(...)</li>
	 *     <li>HAWQSchema.required_group_array(...)</li>
	 *     <li>HAWQSchema.optional_group_array(...)</li>
	 * </ul>
	 *
	 * @param isOptional whether the field is optional or not
	 * @param isArray whether the field is an array or not
	 * @param name name of the field
	 * @param dataTypeName name of the UDT type in HAWQ this group maps to. This is optional.
	 * @param fields fields of the group field
	 */
	public HAWQGroupField(boolean isOptional, boolean isArray, String name,
							 String dataTypeName, HAWQField... fields) {
		this(isOptional, isArray, name, dataTypeName, Arrays.asList(fields));
	}

	/**
	 * Constructor of HAWQGroupField.
	 *
	 * <p>Instead of using this constructor, we recommend you to use
	 * factory methods defined in HAWQSchema:</p>
	 *
	 * <ul>
	 *     <li>HAWQSchema.required_group(...)</li>
	 *     <li>HAWQSchema.optional_group(...)</li>
	 *     <li>HAWQSchema.required_group_array(...)</li>
	 *     <li>HAWQSchema.optional_group_array(...)</li>
	 * </ul>
	 *
	 * @param isOptional whether the field is optional or not
	 * @param isArray whether the field is an array or not
	 * @param name name of the field
	 * @param dataTypeName name of the UDT type in HAWQ this group maps to. This is optional.
	 * @param fields fields of the group field
	 */
	public HAWQGroupField(boolean isOptional, boolean isArray, String name,
							 String dataTypeName, List<HAWQField> fields) {
		super(isOptional, name, isArray);
		// use empty string internally as missing value of data type name
		if (dataTypeName == null)
			dataTypeName = "";
		this.dataTypeName = dataTypeName;
		this.fields = fields;

		this.indexByName = new HashMap<String, Integer>();
		for (int i = 0; i < fields.size(); i++) {
			indexByName.put(fields.get(i).getName(), i);
		}
	}

	/**
	 * Get number of fields this group contains.
	 * @return fields' number
	 */
	public int getFieldCount() {
		return fields.size();
	}

	/**
	 * Get index of a field by field index.
	 * NOTE: field index starts with 1 rather than 0.
	 *
	 * @param fieldName field's name
	 * @return index of field
	 */
	public int getFieldIndex(String fieldName) {
		/*
		 * GPSQL-1031
		 * 
		 * When field is not existed in this schema, throw a readable exception for user
		 */
		if (!indexByName.containsKey(fieldName))
			throw new IllegalArgumentException("Field '" + fieldName + "' not found");
		return indexByName.get(fieldName) + 1;
	}

	/**
	 * Get field by its index in the group.
	 * NOTE: field index starts with 1 rather than 0.
	 *
	 * @param fieldIndex field's index in the group
	 * @return field having the given index
	 */
	public HAWQField getField(int fieldIndex) {
		return fields.get(fieldIndex - 1);
	}

	/**
	 * Get field by field name.
	 * @param fieldName field's name
	 * @return field having the given name.
	 */
	public HAWQField getField(String fieldName) {
		return getField(getFieldIndex(fieldName));
	}

	/**
	 * Get field's type name in lowercase by field's index in the group. Group field's
	 * type name is "group".
	 * NOTE: field index starts with 1 rather than 0.
	 *
	 * @param fieldIndex field's index in the group
	 * @return field's type name in lowercase
	 */
	public String getFieldType(int fieldIndex) {
		HAWQField field = getField(fieldIndex);
		if (field.isPrimitive())
			return field.asPrimitive().getType().toString().toLowerCase();
		return "group";
	}

	/**
	 * Get field's type name in lowercase by field's name. Group field's
	 * type name is "group".
	 * @param fieldName field's name
	 * @return field's type name in lowercase
	 */
	public String getFieldType(String fieldName) {
		return getFieldType(getFieldIndex(fieldName));
	}

	@Override
	protected boolean equalsField(HAWQField other) {
		if (other.isPrimitive()) return false;
		HAWQGroupField g = other.asGroup();
		return  this.isOptional() == g.isOptional() &&
				this.isArray() == g.isArray() &&
				this.getName().equals(g.getName()) &&
				this.getDataTypeName().equals(g.getDataTypeName()) &&
				this.getFields().equals(g.getFields());
	}

	@Override
	public boolean isPrimitive() {
		return false;
	}

	/**
	 * Get all fields of the group.
	 * @return fields of the group
	 */
	public List<HAWQField> getFields() {
		return fields;
	}

	/**
	 * Get data type name of this group field.
	 * @return data type name of the field
	 */
	public String getDataTypeName() {
		return dataTypeName;
	}

	protected void writeMembersToStringBuilder(StringBuilder sb, String indent) {
		for (int i = 0; i < fields.size(); i++) {
			if (i != 0) sb.append("\n");
			fields.get(i).writeToStringBuilder(sb, indent + "  ");
		}
	}

	@Override
	public void writeToStringBuilder(StringBuilder sb, String indent) {
		sb.append(indent)
				.append(isOptional() ? "optional " : "required ")
				.append("group")
				.append(isArray() ? "[] " : " ")
				.append(getName())
				.append(getDataTypeName() == null ? " " : " (" + getDataTypeName() + ") ")
				.append("{\n");

		writeMembersToStringBuilder(sb, indent);

		sb.append("\n").append(indent).append("}");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		writeToStringBuilder(sb, "");
		return sb.toString();
	}
}
