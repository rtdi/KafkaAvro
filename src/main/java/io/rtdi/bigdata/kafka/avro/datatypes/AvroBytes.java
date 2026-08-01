package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.ByteBuffer;
import java.util.Base64;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Is the Avro Type.BYTES datatype, a binary store of any length. A BLOB column.
 *
 */
public class AvroBytes extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this class
	 */
	public static final Factory factory = new Factory();
	/**
	 * Fixed name of this type
	 */
	public static final String NAME = "BYTES";
	private static AvroBytes element = new AvroBytes();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.BYTES));
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
	private AvroBytes() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroBytes create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroBytes create() {
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
		if (schema.getType() != Schema.Type.BYTES) {
			throw new IllegalArgumentException("Logical type must be backed by BYTES");
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
	 * Executes the ByteBuffer convertToInternal operation.
	 * @param value the parameter value
	 */
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
	/**
	 * Executes the byte[] convertToJava operation.
	 * @param value the parameter value
	 */
	public byte[] convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof byte[]) {
			return (byte[]) value;
		} else if (value instanceof ByteBuffer) {
			return ((ByteBuffer) value).array();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a byte[]");
	}

	/**
	 * Factory to create an instance of this class
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
			return AvroBytes.create();
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
			b.append('\"');
			b.append(value.toString());
			b.append('\"');
		}
	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.BYTES;
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
		return AvroType.AVROBYTES;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		byte[] b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + Base64.getEncoder().encodeToString(b) + "\"";
		}
	}

}
