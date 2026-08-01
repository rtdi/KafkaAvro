package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on an Avro Type.INT holds 2-byte signed numbers.
 *
 */
public class AvroShort extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create instances of this class
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of this type
	 */
	public static final String NAME = "SHORT";
	private static AvroShort element = new AvroShort();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.INT));
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
	private AvroShort() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroShort create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroShort create() {
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
		if (schema.getType() != Schema.Type.INT) {
			throw new IllegalArgumentException("Logical type must be backed by an integer");
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
	 * Executes the Integer convertToInternal operation.
	 * @param value the parameter value
	 */
	public Integer convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return validate((Integer) value);
		} else if (value instanceof String) {
			try {
				return validate(Integer.valueOf((String) value));
			} catch (NumberFormatException e) {
				throw new AvroDataTypeException("Cannot convert the string \"" + value + "\" into a Short");
			}
		} else if (value instanceof Number) {
			return validate(((Number) value).intValue());
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into an Integer");
	}

	private Integer validate(Integer value) throws AvroDataTypeException {
		if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
			return value;
		} else {
			throw new AvroDataTypeException("The provided value is outside its bounds for the data type \"Short\": " + Short.MIN_VALUE + " <= " + value + " <= " + Short.MAX_VALUE);
		}
	}

	@Override
	/**
	 * Executes the Short convertToJava operation.
	 * @param value the parameter value
	 */
	public Short convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Short) {
			return (Short) value;
		} else if (value instanceof Integer) {
			return ((Integer) value).shortValue();
		} else if (value instanceof Number) {
			return ((Number) value).shortValue();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Short");
	}

	/**
	 * Factory to create instances of this class
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor of the factory
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
			return AvroShort.create();
		}

	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.INT;
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
		return AvroType.AVROSHORT;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		Short b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return b.toString();
		}
	}

}
