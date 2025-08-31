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
 * A nvarchar is a string up to a provided max length, holds UTF-8 chars
 * and is sorted and compared binary.
 *
 */
public class AvroNVarchar extends LogicalTypeWithLength {
	public static final String NAME = "NVARCHAR";
	public static final Factory factory = new Factory();
	private Schema schema;
	private ObjectMapper om = new ObjectMapper();

	private AvroNVarchar(int length) {
		super(NAME, length);
		this.schema = addToSchema(Schema.create(Type.STRING));
	}

	/**
	 * Create an instance of that type.
	 * @param length of the data type
	 * @return the instance
	 */
	public static AvroNVarchar create(int length) {
		return new AvroNVarchar(length);
	}

	/**
	 * @param text as the textual representation of this data type in the form of NVARCHAR(10)
	 * @return the schema of the logical type
	 */
	public static Schema getSchema(String text) {
		int length = LogicalTypeWithLength.getLengthPortion(text);
		return getSchema(length);
	}

	/**
	 * @param schema with the details of this logical type
	 * @return the instance
	 */
	public static AvroNVarchar create(Schema schema) {
		return new AvroNVarchar(getLengthProperty(schema));
	}

	/**
	 * @param text as the textual data type representation
	 * @return this instance
	 * @throws AvroDataTypeException in case the text has no length portion
	 */
	public static AvroNVarchar create(String text) throws AvroDataTypeException {
		int l = getLengthPortion(text);
		if (l > 0) {
			return new AvroNVarchar(l);
		} else {
			throw new AvroDataTypeException("The supplied data type \"" + text + "\" cannot be parsed into a length portion");
		}
	}

	/**
	 * @param length of the data type
	 * @return the instance
	 */
	public static Schema getSchema(int length) {
		return create(length).addToSchema(Schema.create(Type.STRING));
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
	public void validate(Schema schema) {
		super.validate(schema);
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type " + getName() + " must be backed by string");
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
		if (value.length() <= getLength()) {
			return value;
		} else {
			return value.subSequence(0, getLength());
		}
	}

	public static class Factory implements LogicalTypeFactory {

		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroNVarchar.create(schema);
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
		return AvroType.AVRONVARCHAR;
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
