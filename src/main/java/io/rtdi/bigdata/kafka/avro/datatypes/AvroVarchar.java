package io.rtdi.bigdata.kafka.avro.datatypes;

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
	
	public static AvroVarchar create(int length) {
		return new AvroVarchar(length);
	}

	public static AvroVarchar create(Schema schema) {
		return new AvroVarchar(getLengthProperty(schema));
	}

	public static AvroVarchar create(String text) {
		return new AvroVarchar(getLengthPortion(text));
	}

	public static Schema getSchema(int length) {
		return create(length).addToSchema(Schema.create(Type.STRING));
	}
	
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
	public String convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			return (String) value;
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
		} else {
			return value.toString();
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
