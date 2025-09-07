package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Wrapper around the Avro Type.MAP data type
 *
 */
public class AvroMap extends LogicalType implements IAvroPrimitive {
	/**
	 * Factory instance to be registered with Avro
	 */
	public static final Factory factory = new Factory();
	/**
	 * Name of the logical type
	 */
	public static final String NAME = "MAP";
	private Schema schema;

	/**
	 * @param valueschema with the details of the Map
	 * @return the schema of the logical type
	 */
	public static Schema getSchema(Schema valueschema) {
		AvroMap element = new AvroMap();
		element.schema = Schema.createMap(valueschema);
		return element.addToSchema(element.schema);
	}

	/**
	 * Constructor for this static instance
	 */
	private AvroMap() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @param schema of the entire Map, including the value type
	 * @return the instance
	 */
	public static AvroMap create(Schema schema) {
		AvroMap element = new AvroMap();
		element.schema = schema;
		return element;
	}

	/**
	 * Creates a Map&lt;String, primitive&gt;
	 *
	 * @param primitive the data type for the value part of the map
	 * @return the AvroMap
	 */
	public static AvroMap create(IAvroPrimitive primitive) {
		AvroMap element = new AvroMap();
		element.schema = Schema.createMap(primitive.getDatatypeSchema());
		return element;
	}

	/**
	 * The schema of the entire Map, including the value type
	 *
	 * @return the schema
	 */
	public Schema getSchema() {
		return schema;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.MAP) {
			throw new IllegalArgumentException("Logical type must be backed by a MAP");
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
	public Map<?,?> convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Map) {
			return (Map<?,?>) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Map");
	}

	@Override
	public Map<?,?> convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Map) {
			return (Map<?,?>) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Map");
	}

	/**
	 * Factory class to create an instance of the LogicalType
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Factory constructor
		 */
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroMap.create(schema);
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
		return Type.MAP;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROMAP;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException {
		Map<?, ?> b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			if (schema == null) {
				throw new AvroDataTypeException("Cannot convert to JSON, the schema is not set for the Map datatype");
			}
			StringBuffer sb = new StringBuffer("{");
			Schema mtype = schema.getValueType();
			Schema btype = AvroUtils.getBaseSchema(mtype);
			IAvroDatatype datatype = AvroType.getAvroDataType(btype);
			for (Entry<?, ?> v : b.entrySet()) {
				if (sb.length() > 1) {
					sb.append(',');
				}
				sb.append('\"').append(v.getKey().toString()).append("\":");
				sb.append(datatype.convertToJson(v.getValue()));
			}
			sb.append("}");
			return sb.toString();
		}
	}

}
