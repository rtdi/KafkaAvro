package io.rtdi.bigdata.kafka.avro.datatypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Objects;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper of LogicalTypes.timestampMillis()
 *
 */
public class AvroTimestamp extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory to create an instance of this logical type
	 */
	public static final Factory factory = new Factory();
	private static Schema schema;
	static {
		schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG));
	}
	/**
	 * Name of this type
	 */
	public static final String NAME = "TIMESTAMP";
	private static AvroTimestamp element = new AvroTimestamp();
	private TimestampMillis time = LogicalTypes.timestampMillis();

	/**
	 * creates the schema for this logical type.
	 * @return the static schema of this type
	 */
	public Schema createSchema() {
		return schema;
	}

	/**
	 * Constructor for this static instance
	 */
	private AvroTimestamp() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @return the instance
	 */
	@JsonCreator
	public static AvroTimestamp create() {
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
		if (o == null) {
			return false;
		} else if (o instanceof AvroTimestamp t) {
			return this.getName()== t.getName();
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(time);
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
		} else if (value instanceof Date) {
			return ((Date) value).getTime();
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime v = (ZonedDateTime) value;
			return convertToInternal(v.toInstant());
		} else if (value instanceof Instant) {
			return ((Instant) value).toEpochMilli();
		} else if (value instanceof CharSequence) {
			String s = value.toString();
			try {
				Instant d = Instant.parse(s);
				return d.toEpochMilli();
			} catch (Exception e) {
				throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp, must be an iso timestamp string");
			}
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp");
	}

	@Override
	public Instant convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return Instant.ofEpochMilli((long) value);
		} else if (value instanceof Instant) {
			return (Instant) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp");
	}

	/**
	 * Factory to create an instance of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor of the factory
		 */
		public Factory() {
		}

		@Override
		/**
		 * creates the LogicalType from the provided schema.
		 * @param schema the parameter value
		 */
		public LogicalType fromSchema(Schema schema) {
			return AvroTimestamp.create();
		}

	}

	@Override
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
	public Type getBackingType() {
		return Type.LONG;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROTIMESTAMPMILLIS;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		Instant b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
