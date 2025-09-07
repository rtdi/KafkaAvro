package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Union of multiple types
 */
public class AvroUnion implements IAvroDatatype {
	/**
	 * The name of the type as used in the Avro schema
	 */
	public static final String NAME = "UNION";
	private List<Schema> types;

	/**
	 * Constructor for this static instance
	 */
	private AvroUnion() {
		super();
	}

	private AvroUnion(Schema schema) {
		super();
		this.types = schema.getTypes();
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroUnion create() {
		return new AvroUnion();
	}

	/**
	 * Create an instance of that type based on the schema definition.
	 *
	 * @param schema the schema to create the type from
	 * @return the instance
	 */
	public static IAvroDatatype create(Schema schema) {
		return new AvroUnion(schema);
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public Object convertToInternal(Object value) throws AvroDataTypeException {
		if (value instanceof GenericRecord) {
			return value;
		} else if (value instanceof List<?>) {
			return value;
		} else {
			return value;
		}
	}

	@Override
	public Object convertToJava(Object value) throws AvroDataTypeException {
		if (value instanceof GenericRecord) {
			return value;
		} else if (value instanceof List<?>) {
			return value;
		} else if (value instanceof Utf8) {
			return ((Utf8) value).toString();
		} else {
			return value;
		}
	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append(value);
		}
	}

	@Override
	public Type getBackingType() {
		return Type.UNION;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROUNION;
	}

	/**
	 * Get the list of types that are part of this union
	 *
	 * @return the list of types
	 */
	public List<Schema> getTypes() {
		return types;
	}

	/**
	 * Set the list of types that are part of this union
	 *
	 * @param types the list of types
	 */
	public void setTypes(List<Schema> types) {
		this.types = types;
	}

	/**
	 * Get the schema of the datatype that matches the value's type
	 *
	 * @param value the value to find the schema for
	 * @return the schema or null if not found
	 */
	public Schema getDatatypeSchemaFor(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			GenericRecord r = (GenericRecord) value;
			return r.getSchema();
		} else if (value instanceof EnumSymbol) {
			EnumSymbol s = ((EnumSymbol) value);
			return s.getSchema();
		} else if (value instanceof Fixed) {
			Fixed f = ((Fixed) value);
			return f.getSchema();
		} else if (value instanceof List<?>) {
			return findType(Type.ARRAY);
		} else if (value instanceof CharSequence) {
			return findType(Type.STRING);
		} else if (value instanceof Long) {
			return findType(Type.LONG);
		} else if (value instanceof Float) {
			return findType(Type.FLOAT);
		} else if (value instanceof Double) {
			return findType(Type.DOUBLE);
		} else if (value instanceof Boolean) {
			return findType(Type.BOOLEAN);
		} else if (value instanceof Map) {
			return findType(Type.MAP);
		} else if (value instanceof Integer) {
			return findType(Type.INT);
		} else if (value instanceof byte[] || value instanceof ByteBuffer) {
			return findType(Type.BYTES);
		} else {
			return null;
		}
	}


	@Override
	public String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException {
		if (value == null) {
			return "null";
		} else {
			Schema schema = getDatatypeSchemaFor(value);
			if (schema != null) {
				IAvroDatatype datatype = AvroType.getAvroDataType(schema);
				if (datatype != null) {
					return datatype.convertToJson(value);
				} else {
					throw new AvroDataTypeException("Cannot convert a value of type \"" + schema.getType().getName() + "\" into a JSON string");
				}
			} else {
				throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a JSON string");
			}
		}
	}

	private Schema findType(Type t) {
		for (Schema s : types) {
			if (s.getType() == t) {
				return s;
			}
		}
		return null;
	}
}
