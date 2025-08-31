package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on an Avro Type.NULL
 *
 */
public class AvroNull extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "NULL";
	private static AvroNull element = new AvroNull();


	/**
	 * Constructor for this static instance
	 */
	private AvroNull() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroNull create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
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
		return null;
	}

	@Override
	public Short convertToJava(Object value) throws AvroDataTypeException {
		return null;
	}

	public static class Factory implements LogicalTypeFactory {

		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroNull.create();
		}

	}

	@Override
	public Type getBackingType() {
		return Type.NULL;
	}

	@Override
	public Schema getDatatypeSchema() {
		return Schema.create(Type.NULL);
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVRONULL;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		return "null";
	}

}
