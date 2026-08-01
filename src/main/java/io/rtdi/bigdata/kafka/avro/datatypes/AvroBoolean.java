package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Is the Avro Type.BOOLEAN native type.
 *
 */
public class AvroBoolean extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this logical type
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of the logical type as used in the Avro schema
	 */
	public static final String NAME = "BOOLEAN";
	private static AvroBoolean element = new AvroBoolean();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.BOOLEAN));
	}

	/**
	 * @return the static schema of this type
	 */
	/**
	 * Executes the Schema createSchema operation.
	 */
	public Schema createSchema() {
		return schema;
	}

	/**
	 * Constructor for this static instance
	 */
	private AvroBoolean() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroBoolean create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroBoolean create() {
		return element;
	}

	@Override
	/**
	 * Executes the Schema addToSchema operation.
	 * @param schema the parameter value
	 */
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	/**
	 * Executes the void validate operation.
	 * @param schema the parameter value
	 */
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.BOOLEAN) {
			throw new IllegalArgumentException("Logical type must be backed by a BOOLEAN");
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
		return true;
	}

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return 1;
	}

	@Override
	/**
	 * Executes the String toString operation.
	 */
	public String toString() {
		return NAME;
	}

	@Override
	/**
	 * Executes the Boolean convertToInternal operation.
	 * @param value the parameter value
	 */
	public Boolean convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Boolean) {
			return (Boolean) value;
		} else if (value instanceof String) {
			if ("TRUE".equalsIgnoreCase((String) value)) {
				return Boolean.TRUE;
			} else if ("FALSE".equalsIgnoreCase((String) value)) {
				return Boolean.FALSE;
			}
		} else if (value instanceof Number) {
			int v = ((Number) value).intValue();
			if (v == 1) {
				return Boolean.TRUE;
			} else if (v == 0) {
				return Boolean.FALSE;
			}
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Boolean");
	}

	@Override
	/**
	 * Executes the Boolean convertToJava operation.
	 * @param value the parameter value
	 */
	public Boolean convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Boolean) {
			return (Boolean) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Boolean");
	}

	/**
	 * Factory to create an instance of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Factory constructor
		 */
		/**
		 * Executes the Factory operation.
		 */
		public Factory() {
		}

		@Override
		/**
		 * Executes the LogicalType fromSchema operation.
		 * @param schema the parameter value
		 */
		public LogicalType fromSchema(Schema schema) {
			return AvroBoolean.create();
		}

	}

	@Override
	/**
	 * Executes the void toString operation.
	 * @param b the parameter value
	 * @param value the parameter value
	 */
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append(value.toString());
		}
	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.BOOLEAN;
	}

	@Override
	/**
	 * Executes the Schema getDatatypeSchema operation.
	 */
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	/**
	 * Executes the AvroType getAvroType operation.
	 */
	public AvroType getAvroType() {
		return AvroType.AVROBOOLEAN;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) {
		if (value == null) {
			return "null";
		} else {
			return value.toString();
		}
	}

}
