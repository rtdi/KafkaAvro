package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on an Avro Type.INT holds 2-byte signed numbers.
 *
 */
public class AvroShort extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "SHORT";
	private static AvroShort element = new AvroShort();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.INT));
	}

	/**
	 * @return the static schema of this type
	 */
	public static Schema getSchema() {
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
	public static AvroShort create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.INT) {
			throw new IllegalArgumentException("Logical type must be backed by an integer");
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
		return true;
	}

	@Override
	public int hashCode() {
		return 1;
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append(value.toString());
		}
	}

	@Override
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

	public static class Factory implements LogicalTypeFactory {

		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroShort.create();
		}

	}

	@Override
	public Type getBackingType() {
		return Type.INT;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROSHORT;
	}

}
