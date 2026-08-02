package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * A union of all primitive datatypes, mostly used for extensions.
 *
 */
public class AvroAnyPrimitive implements IAvroPrimitive {
	/**
	 * Name of this datatype
	 */
	public static final String NAME = "ANYPRIMITIVE";
	private static AvroAnyPrimitive element = new AvroAnyPrimitive();
	private static Schema schema;

	static {
		schema =
				Schema.createUnion(
						Schema.create(Type.NULL),
						AvroBoolean.create().createSchema(),
						AvroBytes.create().createSchema(),
						AvroDouble.create().createSchema(),
						AvroFloat.create().createSchema(),
						AvroInt.create().createSchema(),
						AvroLong.create().createSchema(),
						AvroString.create().createSchema());
	}

	/**
	 * Gets the schema of this datatype.
	 * @return the static schema of this type
	 */
	public Schema createSchema() {
		return schema;
	}

	/**
	 * Constructor for this static instance
	 */
	private AvroAnyPrimitive() {
		super();
	}

	@Override
    public String getType() {
        return NAME;
    }

	/**
	 * Create an AvroAnyPrimitive instance.
	 * @return the instance
	 */
	@JsonCreator
	public static AvroAnyPrimitive create() {
		return element;
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
	public Object convertToInternal(Object value) {
		return value;
	}

	@Override
	public Object convertToJava(Object value) throws AvroDataTypeException {
		return value;
	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append(value.toString());
		}
	}

	@Override
	public Type getBackingType() {
		return Type.UNION;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROANYPRIMITIVE;
	}

	@Override
	public String convertToJson(Object value) {
		if (value == null) {
			return "null";
		} else {
			return "\"" + value.toString() + "\"";
		}
	}

}
