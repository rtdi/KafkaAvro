package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

public class AvroUnion implements IAvroDatatype {
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
	 * @param valueschema specifies the datatype of the array
	 * @return the instance
	 */
	public static AvroUnion create() {
		return new AvroUnion();
	}

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

	public List<Schema> getTypes() {
		return types;
	}

	public void setTypes(List<Schema> types) {
		this.types = types;
	}

}
