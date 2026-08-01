package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of the Avro Type.LONG
 *
 */
public class AvroLong extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this class when reading the schema.
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of this type
	 */
	public static final String NAME = "LONG";
	private static AvroLong element = new AvroLong();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.LONG));
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
	private AvroLong() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroLong create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroLong create() {
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
		if (schema.getType() != Schema.Type.LONG) {
			throw new IllegalArgumentException("Logical type must be backed by a LONG");
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
	 * Executes the Long convertToInternal operation.
	 * @param value the parameter value
	 */
	public Long convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return (Long) value;
		} else if (value instanceof String) {
			try {
				return Long.valueOf((String) value);
			} catch (NumberFormatException e) {
				throw new AvroDataTypeException("Cannot convert the string \"" + value + "\" into a Decimal");
			}
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Long");
	}

	@Override
	/**
	 * Executes the Long convertToJava operation.
	 * @param value the parameter value
	 */
	public Long convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return (Long) value;
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Long");
	}

	/**
	 * Factory to create an instance of this class when reading the schema.
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
			return AvroLong.create();
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
		return Type.LONG;
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
		return AvroType.AVROLONG;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		Long b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return b.toString();
		}
	}

}
