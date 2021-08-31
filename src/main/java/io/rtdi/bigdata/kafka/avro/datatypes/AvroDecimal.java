package io.rtdi.bigdata.kafka.avro.datatypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Fixed;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on the Avro Type.BYTES data type and wraps the LogicalTypes.decimal(precision, scale).
 *
 */
public class AvroDecimal extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static final DecimalConversion DECIMAL_CONVERTER = new DecimalConversion();
	public static final String NAME = "decimal";
	private Decimal decimal;
	private Schema schema;

	/**
	 * @param precision number of digits the decimal can hold
	 * @param scale number of digits used for the scale
	 * @return the schema used for this data type
	 */
	public static Schema getSchema(int precision, int scale) {
		return create(precision, scale).addToSchema(Schema.create(Type.BYTES));
	}

	/**
	 * @param text in the form of DECIMAL(p, s)
	 * @return the corresponding AvroDecimal
	 */
	public static Schema getSchema(String text) {
		String[] parts = text.split("[\\(\\)\\,]");
		int precision = 28;
		int scale = 7;
		if (parts.length > 1) {
			precision = Integer.parseInt(parts[1]);
		}
		if (parts.length > 2) {
			scale = Integer.parseInt(parts[2]);
		}
		return getSchema(precision, scale);
	}
	
	/**
	 * @param schema with the decimal details
	 * @return the corresponding AvroDecimal
	 */
	public static AvroDecimal create(Schema schema) {
		return new AvroDecimal(schema);
	}

	/**
	 * @param l based on this Avro native logical type
	 * @return the corresponding AvroDecimal
	 */
	public static AvroDecimal create(Decimal l) {
		return new AvroDecimal(l);
	}

	/**
	 * @param precision number of digits the decimal can hold
	 * @param scale number of digits used for the scale
	 * @return an AvroDecimal with the provided precision and scale 
	 */
	public static AvroDecimal create(int precision, int scale) {
		return new AvroDecimal(precision, scale);
	}

	/**
	 * @param text containing the data type definition as text in the form of DECIMAL(p, s)
	 * @return the corresponding AvroDecimal
	 */
	public static AvroDecimal create(String text) {
		String[] parts = text.split("[\\(\\)\\,]");
		int precision = 28;
		int scale = 7;
		if (parts.length > 1) {
			precision = Integer.parseInt(parts[1]);
		}
		if (parts.length > 2) {
			scale = Integer.parseInt(parts[2]);
		}
		return new AvroDecimal(precision, scale);
	}

	private AvroDecimal(int precision, int scale) {
		super(NAME);
		decimal = LogicalTypes.decimal(precision, scale);
		this.schema = decimal.addToSchema(Schema.create(Type.BYTES));
	}

	/**
	 * Constructor for this static instance
	 * @param schema with the data type details
	 */
	private AvroDecimal(Schema schema) {
		super(NAME);
		decimal = (Decimal) LogicalTypes.fromSchema(schema);
		this.schema = schema;
	}

	/**
	 * Constructor for this static instance
	 * @param l based on this Avro native logical type
	 */
	private AvroDecimal(Decimal l) {
		super(NAME);
		decimal = l;
		this.schema = l.addToSchema(Schema.create(Type.BYTES));
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return decimal.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		decimal.validate(schema);
	}

	@Override
	public boolean equals(Object o) {
		return decimal.equals(o);
	}

	@Override
	public int hashCode() {
		return decimal.hashCode();
	}
	
	@Override
	public String toString() {
		return NAME + "(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
	}

	@Override
	public Object convertToInternal(Object value) throws AvroDataTypeException {
		BigDecimal v = null;
		if (value == null) {
			return null;
		} else {
			if (value instanceof ByteBuffer || value instanceof byte[]) {
				return value;
			} else if (value instanceof Fixed) {
				return ((Fixed) value).bytes();
			} else if (value instanceof BigDecimal) {
				if (decimal.getScale() != ((BigDecimal) value).scale()) {
					v = ((BigDecimal) value).setScale(decimal.getScale(), RoundingMode.HALF_UP);
				} else {
					v = (BigDecimal) value;
				}
				ByteBuffer buffer = DECIMAL_CONVERTER.toBytes(v, null, decimal);
				return buffer;
			} else if (value instanceof Number) {
				Number n = (Number) value;
				// Using the string conversion way to avoid double/float representation errors as much as possible
				v = new BigDecimal(n.toString()).setScale(decimal.getScale(), RoundingMode.HALF_UP);
				ByteBuffer buffer = DECIMAL_CONVERTER.toBytes(v, null, decimal);
				return buffer;
			} else if (value instanceof String) {
				try {
					v = new BigDecimal((String) value);
					ByteBuffer buffer = DECIMAL_CONVERTER.toBytes(v, null, decimal);
					return buffer;
				} catch (NumberFormatException e) {
					throw new AvroDataTypeException("Cannot convert the string \"" + value + "\" into a Decimal");
				}
			}
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Decimal");
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroDecimal.create(schema);
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof ByteBuffer) {
				ByteBuffer v = (ByteBuffer) value;
				if (v.capacity() != 0) {
					v.position(0);
					BigDecimal n = DECIMAL_CONVERTER.fromBytes(v, null, decimal);
					v.position(0);
					b.append(n.toString());
				}
			}
		}
	}

	@Override
	public Type getBackingType() {
		return Type.BYTES;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVRODECIMAL;
	}

	@Override
	public BigDecimal convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof ByteBuffer) {
			return DECIMAL_CONVERTER.fromBytes((ByteBuffer) value, null, decimal);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Decimal");
	}

}
