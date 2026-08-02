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

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on a Avro Type.INT holds the date portion without time.
 * Wraps the Avro LogicalTypes.date().
 *
 */
public class AvroDate extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this logical type
	 */
	public static final Factory factory = new Factory();
	private static Schema schema;
	static {
		schema = LogicalTypes.date().addToSchema(Schema.create(Type.INT));
	}
	/**
	 * The name of this type as used in the schema definition
	 */
	public static final String NAME = "DATE";
	private static AvroDate element = new AvroDate();
	private org.apache.avro.LogicalTypes.Date date = LogicalTypes.date();

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
	private AvroDate() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	/**
	 * Executes the AvroDate create operation and returns the resulting value.
	 * @return the resulting value
	 */
	public static AvroDate create() {
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
		date.validate(schema);
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
		return date.hashCode();
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
		} else if (value instanceof CharSequence) {
			String s = value.toString();
			try {
				LocalDate d = LocalDate.parse(s);
				return (int) d.toEpochDay();
			} catch (Exception e) {
				throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
			}
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
	}

	@Override
	/**
	 * Executes the LocalDate convertToJava operation.
	 * @param value the parameter value
	 */
	public LocalDate convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			long v = ((Integer) value).longValue();
			return LocalDate.ofEpochDay(v);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
	}

	/**
	 * Factory to create an instance of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor for the factory
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
			return AvroDate.create();
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
			if (value instanceof Integer || value instanceof Long) {
				LocalDate d = LocalDate.ofEpochDay(((Number) value).longValue());
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
		return Type.INT;
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
		return AvroType.AVRODATE;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		LocalDate b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
