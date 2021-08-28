package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.ByteBuffer;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

import org.apache.avro.Schema;

/**
 * Wrapper around the Avro Type.Fixed data type
 *
 */
public class AvroFixed extends LogicalTypeWithLength implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "FIXED";
	private Schema schema;

	public AvroFixed(int length) {
		super(NAME, length);
		this.schema = addToSchema(Schema.create(Type.FIXED));
	}

	public static AvroFixed create(int length) {
		return new AvroFixed(length);
	}

	public static Schema getSchema(int length) {
		return create(length).addToSchema(Schema.create(Type.FIXED));
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.FIXED) {
			throw new IllegalArgumentException("Logical type must be backed by a FIXED");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
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
	public ByteBuffer convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof ByteBuffer) {
			return (ByteBuffer) value;
		} else if (value instanceof byte[]) {
			return ByteBuffer.wrap((byte[]) value);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a ByteBuffer");
	}

	@Override
	public byte[] convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof byte[]) {
			return (byte[]) value;
		} else if (value instanceof ByteBuffer) {
			return ((ByteBuffer) value).array();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Fixed");
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroFixed.create(schema.getFixedSize());
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append('\"');
			b.append(value.toString());
			b.append('\"');
		}
	}

	@Override
	public Type getBackingType() {
		return Type.FIXED;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROFIXED;
	}

}
