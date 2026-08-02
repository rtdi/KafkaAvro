package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.ByteBuffer;
import java.util.HashSet;
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
	private List<IAvroDatatype> types;

	/**
	 * Constructor for this static instance
	 */
	/**
	 * Creates a new instance of this class.
	 */
	public AvroUnion() {
		super();
	}

    /**
     * get the name of the type as used in the Avro schema
	 * @return the name of the type
     */
    public String getType() {
        return NAME;
    }

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroUnion create() {
		return new AvroUnion();
	}

	/**
	 * Create an instance of that type.
	 * @param schema the schema to create the union from
	 * @return the instance
	 */
	public static AvroUnion create(Schema schema) {
		AvroUnion element = new AvroUnion();
		element.setTypes(schema.getTypes().stream().map(t -> AvroType.getAvroDataType(t)).toList());
		return element;
	}


	/**
	 * creates the schema for this logical type.
	 * @return the static schema of this type
	 */
	public Schema createSchema() {
		return Schema.createUnion(types.stream().map(t -> t.createSchema()).toList());
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
	public List<IAvroDatatype> getTypes() {
		return types;
	}

	/**
	 * Set the list of types that are part of this union
	 *
	 * @param types the list of types
	 */
	public void setTypes(List<IAvroDatatype> types) {
		this.types = types;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AvroUnion that = (AvroUnion) o;
		return new HashSet<>(types).equals(new HashSet<>(that.types));
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
			return findType(AvroArray.NAME);
		} else if (value instanceof CharSequence) {
			return findType(AvroString.NAME);
		} else if (value instanceof Long) {
			return findType(AvroLong.NAME);
		} else if (value instanceof Float) {
			return findType(AvroFloat.NAME);
		} else if (value instanceof Double) {
			return findType(AvroDouble.NAME);
		} else if (value instanceof Boolean) {
			return findType(AvroBoolean.NAME);
		} else if (value instanceof Map) {
			return findType(AvroMap.NAME);
		} else if (value instanceof Integer) {
			return findType(AvroInt.NAME);
		} else if (value instanceof byte[] || value instanceof ByteBuffer) {
			return findType(AvroBytes.NAME);
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

	private Schema findType(String t) {
		for (IAvroDatatype s : types) {
			if (s.getType().equals(t)) {
				return s.createSchema();
			}
		}
		return null;
	}
}
