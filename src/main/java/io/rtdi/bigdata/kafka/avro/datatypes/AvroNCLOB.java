package io.rtdi.bigdata.kafka.avro.datatypes;

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
 * An AvroNCLOB is an unbounded large UTF-8 character string.
 * @see AvroNVarchar
 *
 */
public class AvroNCLOB extends LogicalType implements IAvroPrimitive {
	/**
	 * Factory to create instances of this class
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of the type as used in the Avro schema
	 */
	public static final String NAME = "NCLOB";
	private static AvroNCLOB element = new AvroNCLOB();
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
	private AvroNCLOB() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroNCLOB create() {
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
			return (CharSequence) value;
		} else {
			return value.toString();
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

	/**
	 * Factory to create instances of this class
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor for the factory
		 */
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroNCLOB.create();
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
		return AvroType.AVRONCLOB;
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
