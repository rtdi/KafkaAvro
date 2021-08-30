package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.charset.StandardCharsets;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * A varchar is a string up to a provided max length, holds ASCII chars only (minus special chars like backspace) 
 * and is sorted and compared binary. 
 *
 */
public class AvroVarchar extends LogicalTypeWithLength {
	public static final Factory factory = new Factory();
	public static final String NAME = "VARCHAR";
	private Schema schema;

	private AvroVarchar(int length) {
		super(NAME, length);
		schema = addToSchema(Schema.create(Type.STRING));
	}
	
	/**
	 * Create an instance of that type.
	 * @param length of the data type
	 * @return the instance
	 */
	public static AvroVarchar create(int length) {
		return new AvroVarchar(length);
	}

	/**
	 * @param schema with the details of this logical type
	 * @return the instance
	 */
	public static AvroVarchar create(Schema schema) {
		return new AvroVarchar(getLengthProperty(schema));
	}

	/**
	 * @param text as the textual data type representation
	 * @return this instance
	 * @throws AvroDataTypeException in case the text has no length portion
	 */
	public static AvroVarchar create(String text) throws AvroDataTypeException {
		int l = getLengthPortion(text);
		if (l > 0) {
			return new AvroVarchar(l);
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
	
	/**
	 * @param text as the textual representation of this data type in the form of VARCHAR(10)
	 * @return the schema of the logical type
	 */
	public static Schema getSchema(String text) {
		int length = LogicalTypeWithLength.getLengthPortion(text);
		return getSchema(length);
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
	public void validate(Schema schema) {
		super.validate(schema);
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type " + getName() + " must be backed by string");
		}
	}

	@Override
	public CharSequence convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof CharSequence) {
			return (CharSequence) value;
		} else {
			return value.toString();
		}
	}

	private CharSequence validate(CharSequence value) throws AvroDataTypeException {
		if (StandardCharsets.US_ASCII.newEncoder().canEncode(value)) {
			if (value.length() <= getLength()) {
				return value;
			} else {
				return value.subSequence(0, getLength());
			}
		} else {
			throw new AvroDataTypeException("The provided value contains non-ASCII chars which is not allowed in a VARCHAR data type");
		}
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroVarchar.create(schema);
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
		return AvroType.AVROVARCHAR;
	}

}
