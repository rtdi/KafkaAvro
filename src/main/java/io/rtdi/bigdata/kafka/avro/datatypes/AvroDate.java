package io.rtdi.bigdata.kafka.avro.datatypes;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Date;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on a Avro Type.INT holds the date portion without time.
 * Wraps the Avro LogicalTypes.date().
 *
 */
public class AvroDate extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static Schema schema;
	static {
		schema = LogicalTypes.date().addToSchema(Schema.create(Type.INT));
	}
	public static final String NAME = "DATE";
	private static AvroDate element = new AvroDate();
	private org.apache.avro.LogicalTypes.Date date = LogicalTypes.date();

	/**
	 * @return the static schema of this type
	 */
	public static Schema getSchema() {
		return schema;
	}

	/**
	 * Constructor for this static instance
	 */
	private AvroDate() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	public static AvroDate create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		date.validate(schema);
	}

	@Override
	public boolean equals(Object o) {
		return date.equals(o);
	}

	@Override
	public int hashCode() {
		return date.hashCode();
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public Object convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return value;
		} else if (value instanceof Number) {
			return ((Number) value).intValue();
		} else if (value instanceof LocalDateTime) {
			return (int) ((LocalDateTime) value).getLong(ChronoField.EPOCH_DAY);
		} else if (value instanceof LocalDate) {
			return (int) ((LocalDate) value).toEpochDay();
		} else if (value instanceof Date) {
			Date d = (Date) value;
			return convertToInternal(Instant.ofEpochMilli(d.getTime()));
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime d = (ZonedDateTime) value;
			return convertToInternal(d.toInstant());
		} else if (value instanceof Instant) {
			Instant d = (Instant) value;
			return (int) LocalDateTime.ofEpochSecond(d.getEpochSecond(), 0, ZoneOffset.UTC).getLong(ChronoField.EPOCH_DAY);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
	}

	@Override
	public LocalDate convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			long v = ((Integer) value).longValue();
			return LocalDate.ofEpochDay(v);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
	}

	public static class Factory implements LogicalTypeFactory {

		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroDate.create();
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof Integer || value instanceof Long) {
				LocalDate d = LocalDate.ofEpochDay(((Number) value).longValue());
				b.append('\"');
				b.append(d.toString());
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
		return AvroType.AVRODATE;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		LocalDate b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
