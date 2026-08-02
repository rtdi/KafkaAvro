package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Some data types have a length attribute like CHAR(10) or VARCHAR(255).
 */
public abstract class LogicalTypeWithLength extends AvroLogicalType implements IAvroPrimitive {
	static final String LENGTH_PROP = "length";

	private int length;

	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 */
	public LogicalTypeWithLength(String name) {
		super(name);
	}

	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param length the length of the data type
	 */
	protected LogicalTypeWithLength(String name, int length) {
		super(name);
		this.length = length;
	}

	/**
	 * set the length portion of the field
	 * @param length length of the data type
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * Get the length portion of the field
	 * @return length of the data type
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Extract the length info from the schema
	 * @param schema of the logical type
	 * @return the extracted length information from the schema
	 */
	@JsonIgnore
	public static Integer getLengthProperty(Schema schema) {
		Object p = schema.getObjectProp(LENGTH_PROP);
		if (p == null) {
			throw new IllegalArgumentException(
					"Schema is missing the length property");
		} else if (p instanceof Integer) {
			return (Integer) p;
		} else if (p instanceof String) {
			try {
				return Integer.valueOf(p.toString());
			} catch (NumberFormatException e) {
			}
		}
		throw new IllegalArgumentException("Expected an integer for length property but got \"" + p.toString() + "\"");
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (length <= 0) {
			throw new IllegalArgumentException("Invalid length: " + length + " (must be positive)");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LogicalTypeWithLength typewithlength = (LogicalTypeWithLength) o;

		if (length != typewithlength.length) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(length);
	}

	@Override
	public Schema addToSchema(Schema schema) {
		super.addToSchema(schema);
		schema.addProp(LENGTH_PROP, length);
		return schema;
	}

	@Override
	public String toString() {
		return getName() + "(" + length + ")";
	}

	/**
	 * Get the length portion of the field from a string representation of the type
	 * The string representation is the toString() of this class
	 * @param text as the string representation of the type
	 * @return the length portion of the field
	 */
	@JsonIgnore
	public static int getLengthPortion(String text) {
		int i = text.indexOf('(');
		int j = text.indexOf(')');
		if (i != -1 && j != -1) {
			String l = text.substring(i+1, j);
			return Integer.valueOf(l);
		} else {
			return -1;
		}
	}
}
