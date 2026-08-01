package io.rtdi.bigdata.kafka.avro.datatypes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Date;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.LogicalTypes.TimeMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTimeMicros extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create instances of this class
	 */
	public static final Factory factory = new Factory();
	private static Schema schema;
	/**
	 * The name of the type as used in the AVRO schema
	 */
	public static final String NAME = "TIMEMICROS";
	private static AvroTimeMicros element = new AvroTimeMicros();
	private TimeMicros time = LogicalTypes.timeMicros();

	static {
		schema = LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG));
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
	 * Constructor for this static instance
	 */
	private AvroTimeMicros() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroTimeMicros create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroTimeMicros create() {
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
		} else if (o instanceof AvroTimeMicros t) {
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
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		} else if (value instanceof LocalTime) {
			LocalTime t = (LocalTime) value;
			return t.getLong(ChronoField.MICRO_OF_DAY);
		} else if (value instanceof LocalDateTime) {
			LocalDateTime t = (LocalDateTime) value;
			return t.getLong(ChronoField.MICRO_OF_DAY);
		} else if (value instanceof Date) {
			Date t = (Date) value;
			return convertToInternal(t.toInstant());
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime t = (ZonedDateTime) value;
			return convertToInternal(t.toInstant());
		} else if (value instanceof Instant) {
			Instant d = (Instant) value;
			return (long) LocalDateTime.ofInstant(d, ZoneOffset.UTC).getLong(ChronoField.MICRO_OF_DAY);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
	}

	@Override
	/**
	 * Executes the LocalTime convertToJava operation.
	 * @param value the parameter value
	 */
	public LocalTime convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return LocalTime.ofNanoOfDay(((Long)value)*1000L);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
	}

	/**
	 * Factory to create instances of this class
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
			return AvroTimeMicros.create();
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
			if (value instanceof Integer) {
				Instant time = Instant.ofEpochMilli((Integer) value);
				b.append('\"');
				b.append(LocalDateTime.ofInstant(time, ZoneOffset.UTC).format(DateTimeFormatter.ISO_TIME));
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
		return AvroType.AVROTIMEMICROS;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		LocalTime b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
