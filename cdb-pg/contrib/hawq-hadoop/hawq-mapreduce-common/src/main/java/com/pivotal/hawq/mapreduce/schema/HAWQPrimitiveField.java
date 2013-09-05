package com.pivotal.hawq.mapreduce.schema;

/**
 * User: gaod1
 * Date: 8/26/13
 */
public class HAWQPrimitiveField extends HAWQField {

	public static enum PrimitiveType {
		BOOL, BIT, VARBIT, BYTEA, INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC,
		CHAR, BPCHAR, VARCHAR, TEXT, DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL,
		POINT, LSEG, BOX, CIRCLE, PATH, POLYGON, MACADDR, INET, CIDR, XML
	}

	private PrimitiveType type;

	protected HAWQPrimitiveField(boolean isOptional, String name, PrimitiveType type, boolean isArray) {
		super(isOptional, name, isArray);
		// use BPCHAR internally
		if (type == PrimitiveType.CHAR) {
			type = PrimitiveType.BPCHAR;
		}
		this.type = type;
	}

	@Override
	protected boolean equalsField(HAWQField other) {
		if (other.isPrimitive()) {
			HAWQPrimitiveField p = other.asPrimitive();
			return  this.isOptional() == p.isOptional() &&
					this.getType() == p.getType() &&
					this.isArray() == p.isArray() &&
					this.getName().equals(p.getName());
		}
		return false;
	}

	@Override
	public boolean isPrimitive() {
		return true;
	}

	public PrimitiveType getType() {
		return type;
	}

	@Override
	public void writeToStringBuilder(StringBuilder sb, String indent) {
		sb.append(indent)
				.append(isOptional() ? "optional " : "required ")
				.append(getType().toString().toLowerCase())
				.append(isArray() ? "[] " : " ")
				.append(getName())
				.append(";");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("HAWQPrimitiveField { ");
		this.writeToStringBuilder(sb, "");
		sb.append(" }");
		return sb.toString();
	}
}
