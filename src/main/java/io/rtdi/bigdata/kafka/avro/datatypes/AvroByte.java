package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on an INT but is supposed to hold data from -127 to +128 only. A single signed byte.
 *
 */
public class AvroByte extends LogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this logical type
	 */
	public static final Factory factory = new Factory();
	/**
	 * Fixed name of this logical type
	 */
	public static final String NAME = "BYTE";
	private static AvroByte element = new AvroByte();
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
	private AvroByte() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroByte create() {
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
				throw new AvroDataTypeException("Cannot convert the string \"" + value + "\" into a Integer");
			}
		} else if (value instanceof Number) {
			return validate(((Number) value).intValue());
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Byte");
	}

	private Integer validate(Integer value) throws AvroDataTypeException {
		if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
			return value;
		} else {
			throw new AvroDataTypeException("The provided value is outside its bounds for the data type \"Byte\": " + Byte.MIN_VALUE + " <= " + value + " <= " + Byte.MAX_VALUE);
		}
	}

	/**
	 * Factory to create an instance of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Factory constructor
		 */
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroByte.create();
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
		return AvroType.AVROBYTE;
	}

	@Override
	public Byte convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Byte) {
			return (Byte) value;
		} else if (value instanceof Integer) {
			return ((Integer) value).byteValue();
		} else if (value instanceof Number) {
			return ((Number) value).byteValue();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Byte");
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		Byte b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return b.toString();
		}
	}

}
