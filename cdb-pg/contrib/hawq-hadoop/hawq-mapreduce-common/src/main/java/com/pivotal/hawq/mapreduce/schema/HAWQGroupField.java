package com.pivotal.hawq.mapreduce.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: gaod1
 * Date: 8/26/13
 */
public class HAWQGroupField extends HAWQField {

	private List<HAWQField> fields;
	private String dataTypeName; /* group maps to UDT in HAWQ, thus this is the name of the UDT */

	private Map<String, Integer> indexByName;

	protected HAWQGroupField(boolean isOptional, boolean isArray, String name,
							 String dataTypeName, HAWQField... fields) {
		this(isOptional, isArray, name, dataTypeName, Arrays.asList(fields));
	}

	protected HAWQGroupField(boolean isOptional, boolean isArray, String name,
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

	public int getFieldCount() {
		return fields.size();
	}

	/**
	 * Get index of the field with given name.
	 * NOTE: field index starts with 1 instead of 0.
	 *
	 * @param fieldName
	 * @return index of field
	 */
	public int getFieldIndex(String fieldName) {
		return indexByName.get(fieldName) + 1;
	}

	public HAWQField getField(int fieldIndex) {
		return fields.get(fieldIndex - 1);
	}

	public HAWQField getField(String fieldName) {
		return getField(getFieldIndex(fieldName));
	}

	public String getFieldType(int fieldIndex) {
		HAWQField field = getField(fieldIndex);
		if (field.isPrimitive())
			return field.asPrimitive().getType().toString().toLowerCase();
		return "group";
	}

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

	public List<HAWQField> getFields() {
		return fields;
	}

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
