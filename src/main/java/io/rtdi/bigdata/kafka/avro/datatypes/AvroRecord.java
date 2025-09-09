package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Generic Avro Record
 */
public class AvroRecord implements IAvroDatatype {
	/**
	 * The name of this datatype
	 */
	public static final String NAME = "RECORD";
	static AvroRecord element = new AvroRecord();

	private AvroRecord() {
	}

	/**
	 * Static factory method to create an instance of this datatype
	 *
	 * @return an instance of AvroRecord
	 */
	public static AvroRecord create() {
		return element;
	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value instanceof Record) {
			Record r = (Record) value;
			Schema schema = r.getSchema();
			b.append('{');
			boolean first = true;
			for (Field f : schema.getFields()) {
				Object v = r.get(f.pos());
				if (v != null) {
					IAvroDatatype datatype = AvroType.getAvroDataType(f.schema());
					if (datatype != null) {
						if (!first) {
							b.append(',');
						} else {
							first = false;
						}
						b.append('\"');
						b.append(f.name());
						b.append("\":");
						datatype.toString(b, v);
					}
				}
			}
			b.append('}');
		}
	}

	@Override
	public GenericRecord convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			return (GenericRecord) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericRecord");
	}

	@Override
	public GenericRecord convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			return (GenericRecord) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericRecord");
	}

	@Override
	public Type getBackingType() {
		return Type.RECORD;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVRORECORD;
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException {
		GenericRecord b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			StringBuffer sb = new StringBuffer("{");
			for (Field f : b.getSchema().getFields()) {
				Object v = b.get(f.pos());
				IAvroDatatype datatype = AvroType.getAvroDataType(AvroUtils.getBaseSchema(f.schema()));
				if (sb.length() > 1) {
					sb.append(',');
				}
				sb.append('\"');
				sb.append(f.name());
				sb.append("\":");
				sb.append(datatype.convertToJson(v));
			}
			sb.append("}");
			return sb.toString();
		}
	}

}
