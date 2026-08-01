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
public class AvroMap extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory instance to be registered with Avro
	 */
	public static final Factory factory = new Factory();
	/**
	 * Name of the logical type
	 */
	public static final String NAME = "MAP";
	private Schema schema;
	private IAvroDatatype datatype;

	/**
	 * @param valueschema with the details of the Map
	 * @return the schema of the logical type
	 */
	/**
	 * Executes the Schema getSchema operation and returns the resulting value.
	 * @param valueschema the parameter value
	 * @return the resulting value
	 */
	public static Schema getSchema(Schema valueschema) {
		return create(valueschema).createSchema();
	}

	/**
	 * Constructor for this static instance
	 * @param valueschema complete schema definition for this data type
	 */
	private AvroMap(Schema valueschema) {
		super(NAME);
		this.schema = addToSchema(Schema.createMap(valueschema));
	}

	/**
	 * Constructor for this static instance
	 */
	/**
	 * Creates a new instance of this class.
	 */
	public AvroMap() {
		super(NAME);
	}

	/**
	 * Creates a new instance of this class.
	 * @param datatype the parameter value
	 */
	public AvroMap(IAvroDatatype datatype) {
		super(NAME);
		this.datatype = datatype;
		this.schema = addToSchema(Schema.createMap(datatype.createSchema()));
	}

	/**
	 * Executes the void setDatatype operation.
	 * @param datatype the parameter value
	 */
	public void setDatatype(IAvroDatatype datatype) {
		this.datatype = datatype;
		this.schema = addToSchema(Schema.createMap(datatype.createSchema()));
	}

	/**
	 * Executes the IAvroDatatype getDatatype operation.
	 */
	public IAvroDatatype getDatatype() {
		return datatype;
	}

	/**
	 * Create an instance of that type.
	 * @param schema of the entire Map, including the value type
	 * @return the instance
	 */
	/**
	 * Executes the AvroMap create operation and returns the resulting value.
	 * @param schema the parameter value
	 * @return the resulting value
	 */
	public static AvroMap create(Schema schema) {
		return new AvroMap(schema);
	}

	/**
	 * Creates a Map&lt;String, primitive&gt;
	 *
	 * @param primitive the data type for the value part of the map
	 * @return the AvroMap
	 */
	/**
	 * Executes the AvroMap create operation and returns the resulting value.
	 * @param primitive the parameter value
	 * @return the resulting value
	 */
	public static AvroMap create(IAvroPrimitive primitive) {
		return create(primitive.getDatatypeSchema());
	}

	/**
	 * The schema of the entire Map, including the value type
	 *
	 * @return the schema
	 */
	/**
	 * Executes the Schema createSchema operation.
	 */
	public Schema createSchema() {
		return schema;
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
		if (schema.getType() != Schema.Type.MAP) {
			throw new IllegalArgumentException("Logical type must be backed by a MAP");
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
			return AvroMap.create(schema);
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
		return Type.MAP;
	}

	@Override
	/**
	 * Executes the Schema getDatatypeSchema operation.
	 */
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	/**
	 * Executes the AvroType getAvroType operation.
	 */
	public AvroType getAvroType() {
		return AvroType.AVROMAP;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
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
