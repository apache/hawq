package com.pivotal.hawq.mapreduce.schema;

/**
 * User: gaod1
 * Date: 8/26/13
 */
public abstract class HAWQField {

	private boolean isOptional;
	private String name;
	private boolean isArray;

	protected HAWQField(boolean isOptional, String name, boolean isArray) {
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

	protected abstract boolean equalsField(HAWQField other);

	abstract public boolean isPrimitive();

	abstract public void writeToStringBuilder(StringBuilder sb, String indent);

	public HAWQPrimitiveField asPrimitive() {
		if (!isPrimitive()) {
			throw new ClassCastException(this + " is not a primitive field");
		}
		return (HAWQPrimitiveField) this;
	}

	public HAWQGroupField asGroup() {
		if (isPrimitive()) {
			throw new ClassCastException(this + " is not a group field");
		}
		return (HAWQGroupField) this;
	}

	public boolean isOptional() {
		return isOptional;
	}

	public String getName() {
		return name;
	}

	public boolean isArray() {
		return isArray;
	}
}
