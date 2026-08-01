package io.rtdi.bigdata.kafka.avro.datatypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.LogicalTypes.TimestampMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of LogicalTypes.timestampMillis()
 *
 */
public class AvroTimestampMicros extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this logical type
	 */
	public static final Factory factory = new Factory();
	private static Schema schema;
	static {
		schema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG));
	}
	/**
	 * The name of this type as used in the schema
	 */
	public static final String NAME = "TIMESTAMPMICROS";
	private static AvroTimestampMicros element = new AvroTimestampMicros();
	private TimestampMicros time = LogicalTypes.timestampMicros();

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
	 * Constructor for this static instance
	 */
	private AvroTimestampMicros() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroTimestampMicros create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroTimestampMicros create() {
		return element;
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
		time.validate(schema);
	}

	@Override
	/**
	 * Executes the boolean equals operation.
	 * @param o the parameter value
	 */
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		} else if (o instanceof AvroTimestampMicros t) {
			return this.getName()== t.getName();
		} else {
			return false;
		}
	}

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return time.hashCode();
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
	 * Executes the Long convertToInternal operation.
	 * @param value the parameter value
	 */
	public Long convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return (Long) value;
		} else if (value instanceof Date) {
			return ((Date) value).getTime() * 1000L;
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime v = (ZonedDateTime) value;
			return convertToInternal(v.toInstant());
		} else if (value instanceof Instant) {
			Instant i = (Instant) value;
			return i.getEpochSecond() * 1000000L + i.getNano()/1000;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros");
	}

	@Override
	/**
	 * Executes the Instant convertToJava operation.
	 * @param value the parameter value
	 */
	public Instant convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			long l = (long) value;
			return Instant.ofEpochSecond(l / 1000000L, (l % 1000000) * 1000L);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros");
	}

	/**
	 * Factory to create the logical type from a schema
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor
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
			return AvroTimestampMicros.create();
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
			if (value instanceof Long) {
				Date d = new Date((Long) value);
				b.append('\"');
				b.append(d.toString());
				b.append('\"');
			}
		}
	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.LONG;
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
		return AvroType.AVROTIMESTAMPMICROS;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		Instant b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
