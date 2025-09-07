package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.charset.StandardCharsets;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Based on a Avro Type.STRING, used to indicate this is a very long text of ASCII characters.
 * In other words, the unbounded version of an AvroVarchar().
 *
 */
public class AvroCLOB extends LogicalType implements IAvroPrimitive {
	/**
	 * Factory to create instances of this logical type
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of the logical type as used in the Avro schema
	 */
	public static final String NAME = "CLOB";
	private static AvroCLOB element = new AvroCLOB();
	private static Schema schema;
	private ObjectMapper om = new ObjectMapper();

	static {
		schema = create().addToSchema(Schema.create(Type.STRING));
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
	private AvroCLOB() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroCLOB create() {
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
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type must be backed by a string");
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
			b.append('\"');
			b.append(AvroUtils.encodeJson(value.toString()));
			b.append('\"');
		}
	}

	@Override
	public CharSequence convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof CharSequence) {
			return validate((CharSequence) value);
		} else {
			return validate(value.toString());
		}
	}

	@Override
	public String convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			return (String) value;
		} else if (value instanceof Utf8) {
			return ((Utf8) value).toString();
		} else if (value instanceof CharSequence) {
			return value.toString();
		} else {
			return value.toString();
		}
	}

	private CharSequence validate(CharSequence value) throws AvroDataTypeException {
		if (StandardCharsets.US_ASCII.newEncoder().canEncode(value)) {
			return value;
		} else {
			throw new AvroDataTypeException("The provided value contains non-ASCII chars which is not allowed in a VARCHAR data type");
		}
	}

	/**
	 * Factory to create instances of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor for the factory
		 */
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroCLOB.create();
		}

	}

	@Override
	public Type getBackingType() {
		return Type.STRING;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROCLOB;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException {
		String b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return om.writeValueAsString(b);
		}
	}

}
