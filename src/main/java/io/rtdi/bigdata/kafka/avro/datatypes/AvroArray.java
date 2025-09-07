package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Wrapper around the Avro Type.MAP data type
 *
 */
public class AvroArray extends LogicalType implements IAvroPrimitive {
	/**
	 * Factory instance to be registered with Avro
	 */
	public static final Factory factory = new Factory();
	/**
	 * The name of this logical type
	 */
	public static final String NAME = "ARRAY";
	private Schema schema;

	/**
	 * @param valueschema complete schema definition for this data type
	 * @return the schema of this type
	 */
	public static Schema getSchema(Schema valueschema) {
		return create(valueschema).getSchema();
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
	private AvroArray() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @param valueschema specifies the datatype of the array
	 * @return the instance
	 */
	public static AvroArray create(Schema valueschema) {
		return new AvroArray(valueschema);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroArray create() {
		return new AvroArray();
	}

	/**
	 * @param primitive contains the data type of the array items
	 * @return AvroArray using the primitive type as list data type
	 */
	public static AvroArray create(IAvroPrimitive primitive) {
		return create(primitive.getDatatypeSchema());
	}

	/**
	 * @return the Array's schema
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
		if (schema.getType() != Schema.Type.ARRAY) {
			throw new IllegalArgumentException("Logical type must be backed by an ARRAY");
		}
	}

	@Override
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

	/**
	 * Factory to create the logical type during Avro schema parsing
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor to register the factory with Avro
		 */
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroArray.create(schema);
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof List) {
				List<?> l = (List<?>) value;
				if (l.size() > 0) {
					b.append('[');
					boolean first = true;
					Object row1 = l.get(0);
					if (row1 instanceof Record) {
						AvroRecord recorddatatype = AvroRecord.create();
						for (Object o : l) {
							if (!first) {
								b.append(',');
							} else {
								first = false;
							}
							recorddatatype.toString(b, o);
						}
					} else {
						// This is not a perfect implementation as it renders the base Avro datatype only, e.g ByteBuffer and not BigDecimal
						for (Object o : l) {
							if (!first) {
								b.append(',');
							} else {
								first = false;
							}
							b.append(o.toString());
						}
					}
					b.append(']');
				}
			}
		}
	}

	@Override
	public Type getBackingType() {
		return Type.ARRAY;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROARRAY;
	}

	@Override
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
