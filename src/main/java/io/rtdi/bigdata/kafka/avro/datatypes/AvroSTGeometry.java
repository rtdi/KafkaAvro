package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Type.STRING backed representation of geospatial data in WKT format.
 *
 */
public class AvroSTGeometry extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "ST_GEOMETRY";
	private static AvroSTGeometry element = new AvroSTGeometry();
	private static Schema schema;
	
	static {
		schema = create().addToSchema(Schema.create(Type.STRING));
	}

	public static Schema getSchema() {
		return schema;
	}

	private AvroSTGeometry() {
		super(NAME);
	}
	
	public static AvroSTGeometry create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type must be backed by a string");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
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
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append('\"');
			b.append(value.toString());
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
			return AvroSTGeometry.create();
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
		return AvroType.AVROSTGEOMETRY;
	}

}
