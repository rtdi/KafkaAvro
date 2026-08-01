package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

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
	 * @return the static schema of this type
	 */
	/**
	 * Executes the Schema createSchema operation.
	 */
	public Schema createSchema() {
		return schema;
	}

	/**
	 * Executes the String getName operation.
	 */
	public String getName() {
		return NAME;
	}
	
    /**
     * Executes the String getType operation.
     */
    public String getType() {
        return getName();
    }

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	/**
	 * Executes the AvroAnyPrimitive create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroAnyPrimitive create() {
		return element;
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
	/**
	 * Executes the Object convertToInternal operation.
	 * @param value the parameter value
	 */
	public Object convertToInternal(Object value) {
		return value;
	}

	@Override
	/**
	 * Executes the Object convertToJava operation.
	 * @param value the parameter value
	 */
	public Object convertToJava(Object value) throws AvroDataTypeException {
		return value;
	}

	@Override
	/**
	 * Executes the void toString operation.
	 * @param b the parameter value
	 * @param value the parameter value
	 */
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append(value.toString());
		}
	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.UNION;
	}

	@Override
	/**
	 * Executes the Schema getDatatypeSchema operation.
	 */
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	/**
	 * Executes the AvroType getAvroType operation.
	 */
	public AvroType getAvroType() {
		return AvroType.AVROANYPRIMITIVE;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) {
		if (value == null) {
			return "null";
		} else {
			return "\"" + value.toString() + "\"";
		}
	}

}
