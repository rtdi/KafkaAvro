package io.rtdi.bigdata.kafka.avro.datatypes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LocalTimestampMillis;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of LogicalTypes.timestampMillis()
 *
 */
public class AvroLocalTimestamp extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this logical type
	 */
	public static final Factory factory = new Factory();
	private static Schema schema;
	static {
		schema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Type.LONG));
	}
	/**
	 * The name of the logical type as used in the Avro schema
	 */
	public static final String NAME = "LOCALTIMESTAMP";
	private static AvroLocalTimestamp element = new AvroLocalTimestamp();
	private LocalTimestampMillis time = LogicalTypes.localTimestampMillis();

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
	private AvroLocalTimestamp() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroLocalTimestamp create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroLocalTimestamp create() {
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
		} else if (o instanceof AvroLocalTimestamp t) {
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
			return ((Date) value).getTime();
		} else if (value instanceof LocalDateTime) {
			LocalDateTime v = (LocalDateTime) value;
			return convertToInternal(v.toInstant(ZoneOffset.UTC));
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime v = (ZonedDateTime) value;
			return convertToInternal(v.toInstant());
		} else if (value instanceof Instant) {
			return ((Instant) value).toEpochMilli();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a LocalDateTime");
	}

	@Override
	/**
	 * Executes the LocalDateTime convertToJava operation.
	 * @param value the parameter value
	 */
	public LocalDateTime convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			long l = (long) value;
			return LocalDateTime.ofEpochSecond(l/1000L, ((int) (l % 1000L)) * 1_000_000 , ZoneOffset.UTC);
		} else if (value instanceof LocalDateTime) {
			return (LocalDateTime) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a LocalDateTime");
	}

	/**
	 * Factory to create an instance of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor of the factory
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
			return AvroLocalTimestamp.create();
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
		return AvroType.AVROLOCALTIMESTAMPMILLIS;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		LocalDateTime b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
