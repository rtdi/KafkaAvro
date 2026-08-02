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
	 * creates the schema for this logical type.
	 * @return the static schema of this type
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
	public static AvroTimestampMicros create() {
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
		} else if (o instanceof AvroTimestampMicros t) {
			return this.getName()== t.getName();
		} else {
			return false;
		}
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
		} else if (value instanceof Date) {
			return ((Date) value).getTime() * 1000L;
		} else if (value instanceof ZonedDateTime) {
			ZonedDateTime v = (ZonedDateTime) value;
			return convertToInternal(v.toInstant());
		} else if (value instanceof Instant) {
			Instant i = (Instant) value;
			return i.getEpochSecond() * 1000000L + i.getNano()/1000;
		} else if (value instanceof CharSequence) {
			String s = value.toString();
			try {
				Instant d = Instant.parse(s);
				return d.getEpochSecond() * 1000000L + d.getNano()/1000;
			} catch (Exception e) {
				throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros, must be an iso timestamp string");
			}
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros");
	}

	@Override
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
		public Factory() {
		}

		@Override
		/**
		 * creates the LogicalType from the provided schema.
		 * @param schema the parameter value
		 */
		public LogicalType fromSchema(Schema schema) {
			return AvroTimestampMicros.create();
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
		return AvroType.AVROTIMESTAMPMICROS;
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
