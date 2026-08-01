package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.Arrays;
import java.util.List;

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
public class AvroArray extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory instance to be registered with Avro
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of this logical type
	 */
	public static final String NAME = "ARRAY";
	private Schema schema;
	private IAvroDatatype datatype;

	/**
	 * @param valueschema complete schema definition for this data type
	 * @return the schema of this type
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
	private AvroArray(Schema valueschema) {
		super(NAME);
		this.schema = addToSchema(Schema.createArray(valueschema));
	}

	/**
	 * Constructor for this static instance
	 */
	/**
	 * Creates a new instance of this class.
	 */
	public AvroArray() {
		super(NAME);
	}

	/**
	 * Creates a new instance of this class.
	 * @param datatype the parameter value
	 */
	public AvroArray(IAvroDatatype datatype) {
		super(NAME);
		this.datatype = datatype;
		this.schema = addToSchema(Schema.createArray(datatype.createSchema()));
	}

	/**
	 * Create an instance of that type.
	 * @param valueschema specifies the datatype of the array
	 * @return the instance
	 */
	/**
	 * Executes the AvroArray create operation and returns the resulting value.
	 * @param valueschema the parameter value
	 * @return the resulting value
	 */
	public static AvroArray create(Schema valueschema) {
		return new AvroArray(valueschema);
	}

	/**
	 * Executes the IAvroDatatype getDatatype operation.
	 */
	public IAvroDatatype getDatatype() {
		return datatype;
	}

	/**
	 * Executes the void setDatatype operation.
	 * @param datatype the parameter value
	 */
	public void setDatatype(IAvroDatatype datatype) {
		this.datatype = datatype;
		this.schema = addToSchema(Schema.createArray(datatype.createSchema()));
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	/**
	 * Executes the AvroArray create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroArray create() {
		return new AvroArray();
	}

	/**
	 * @param primitive contains the data type of the array items
	 * @return AvroArray using the primitive type as list data type
	 */
	/**
	 * Executes the AvroArray create operation and returns the resulting value.
	 * @param primitive the parameter value
	 * @return the resulting value
	 */
	public static AvroArray create(IAvroPrimitive primitive) {
		return create(primitive.getDatatypeSchema());
	}

	/**
	 * @return the Array's schema
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
		if (schema.getType() != Schema.Type.ARRAY) {
			throw new IllegalArgumentException("Logical type must be backed by an ARRAY");
		}
	}

	@Override
	/**
	 * Executes the String toString operation.
	 */
	public String toString() {
		return NAME;
	}

	@Override
	public List<?> convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof List) {
			return (List<?>) value;
		} else if (value instanceof Object[]) {
			Object[] v = (Object[]) value;
			return Arrays.asList(v);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a List");
	}

	@Override
	public List<?> convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof List) {
			return (List<?>) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a List");
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
		AvroArray that = (AvroArray) o;
		return datatype.equals(that.datatype);
	}

	/**
	 * Factory to create the logical type during Avro schema parsing
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor to register the factory with Avro
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
			return AvroArray.create(schema);
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
			if (value instanceof List) {
				List<?> l = (List<?>) value;
				if (l.size() > 0) {
					b.append('[');
					boolean first = true;
					for (Object o : l) {
						if (!first) {
							b.append(',');
						} else {
							first = false;
						}
						b.append(o.toString());
					}
					b.append(']');
				}
			}
		}
	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.ARRAY;
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
		return AvroType.AVROARRAY;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException {
		List<?> b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			if (schema == null) {
				throw new AvroDataTypeException("Cannot convert to JSON, the schema is not set for the Array datatype");
			}
			StringBuffer sb = new StringBuffer("[");
			IAvroDatatype datatype = AvroType.getAvroDataType(AvroUtils.getBaseSchema(schema.getElementType()));
			for (Object v : b) {
				if (sb.length() > 1) {
					sb.append(',');
				}
				sb.append(datatype.convertToJson(v));
			}
			sb.append("]");
			return sb.toString();
		}
	}

}
