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
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTime extends LogicalType implements IAvroPrimitive {
	/**
	 * Factory to create instances of this class
	 */
	public static final Factory factory = new Factory();
	private static Schema schema;
	/**
	 * The name of the type as used in the AVRO schema
	 */
	public static final String NAME = "TIME";
	private static AvroTime element = new AvroTime();
	private TimeMillis time = LogicalTypes.timeMillis();

	static {
		schema = LogicalTypes.timeMillis().addToSchema(Schema.create(Type.INT));
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
	private AvroTime() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroTime create() {
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
	public Integer convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return (Integer) value;
		} else if (value instanceof Number) {
			return ((Number) value).intValue();
		} else if (value instanceof LocalTime) {
			LocalTime t = (LocalTime) value;
			return (int) t.getLong(ChronoField.MILLI_OF_DAY);
		} else if (value instanceof LocalDateTime) {
			LocalDateTime t = (LocalDateTime) value;
			return (int) t.getLong(ChronoField.MILLI_OF_DAY);
		} else if (value instanceof Date) {
			Date t = (Date) value;
			return convertToInternal(t.toInstant());
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime t = (ZonedDateTime) value;
			return convertToInternal(t.toInstant());
		} else if (value instanceof Instant) {
			Instant d = (Instant) value;
			return (int) LocalDateTime.ofInstant(d, ZoneOffset.UTC).getLong(ChronoField.MILLI_OF_DAY);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
	}

	@Override
	public LocalTime convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return LocalTime.ofNanoOfDay(Long.valueOf((Integer) value) * 1000000L);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
	}

	/**
	 * Factory to create instances of this class
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor of the factory
		 */
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroTime.create();
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
		return Type.INT;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROTIMEMILLIS;
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
