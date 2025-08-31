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

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTimeMicros extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static Schema schema;
	public static final String NAME = "TIMEMICROS";
	private static AvroTimeMicros element = new AvroTimeMicros();
	private TimeMicros time = LogicalTypes.timeMicros();

	static {
		schema = LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG));
	}

	/**
	 * @return the static schema of this type
	 */
	public static Schema getSchema() {
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
	public static AvroTimeMicros create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		time.validate(schema);
	}

	@Override
	public boolean equals(Object o) {
		return time.equals(o);
	}

	@Override
	public int hashCode() {
		return time.hashCode();
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
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
	public LocalTime convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return LocalTime.ofNanoOfDay(((Long)value)*1000L);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
	}

	public static class Factory implements LogicalTypeFactory {

		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroTimeMicros.create();
		}

	}

	@Override
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
	public Type getBackingType() {
		return Type.LONG;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROTIMEMICROS;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		LocalTime b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
