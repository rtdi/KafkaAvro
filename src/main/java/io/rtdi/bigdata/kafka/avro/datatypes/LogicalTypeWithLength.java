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

	protected LogicalTypeWithLength(String name, int length) {
		super(name);
		this.length = length;
	}

	
	/**
	 * Executes the void setLength operation.
	 * @param length the parameter value
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * @return length of the data type
	 */
	/**
	 * Executes the int getLength operation.
	 */
	public int getLength() {
		return length;
	}

	/**
	 * @param schema of the logical type
	 * @return the extracted length information from the schema
	 */
	@JsonIgnore
	/**
	 * Executes the Integer getLengthProperty operation and returns the resulting value.
	 * @param schema the parameter value
	 * @return the resulting value
	 */
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
	/**
	 * Executes the void validate operation.
	 * @param schema the parameter value
	 */
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (length <= 0) {
			throw new IllegalArgumentException("Invalid length: " + length + " (must be positive)");
		}
	}

	@Override
	/**
	 * Executes the boolean equals operation.
	 * @param o the parameter value
	 */
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
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return Integer.hashCode(length);
	}

	@Override
	/**
	 * Executes the Schema addToSchema operation.
	 * @param schema the parameter value
	 */
	public Schema addToSchema(Schema schema) {
		super.addToSchema(schema);
		schema.addProp(LENGTH_PROP, length);
		return schema;
	}

	@Override
	/**
	 * Executes the String toString operation.
	 */
	public String toString() {
		return getName() + "(" + length + ")";
	}

	/**
	 * @param text of the data type like VARCHAR(10)
	 * @return the length attribute inside above text or -1 if none provided
	 */
	@JsonIgnore
	/**
	 * Executes the int getLengthPortion operation and returns the resulting value.
	 * @param text the parameter value
	 * @return the resulting value
	 */
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
