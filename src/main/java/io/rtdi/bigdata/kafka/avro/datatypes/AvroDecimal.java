package io.rtdi.bigdata.kafka.avro.datatypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Fixed;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Based on the Avro Type.BYTES data type and wraps the LogicalTypes.decimal(precision, scale).
 *
 */
public class AvroDecimal extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory instance to be used when registering this logical type
	 */
	public static final Factory factory = new Factory();
	private static final DecimalConversion DECIMAL_CONVERTER = new DecimalConversion();
	/**
	 * The name of this logical type as used in the schema
	 */
	public static final String NAME = "decimal";
	private Decimal decimal;
	private Schema schema;
	private Integer precision;
	private Integer scale;

	/**
	 * creates the schema of this datatype.
	 * @param precision number of digits the decimal can hold
	 * @param scale number of digits used for the scale
	 * @return the schema used for this data type
	 */
	public static Schema getSchema(int precision, int scale) {
		return create(precision, scale).addToSchema(Schema.create(Type.BYTES));
	}

	/**
	 * Creates a new instance of this class.
	 */
	public AvroDecimal() {
		super(NAME);
	}

	
	/**
	 * Get the decimal precision.
	 * @return the decimal precision
	 */
	public Integer getPrecision() {
		return precision;
	}

	/**
	 * Set the decimal precision.
	 * @param precision the decimal precision
	 */
	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	/**
	 * Get the decimal scale.
	 * @return the decimal scale
	 */
	public Integer getScale() {
		return scale;
	}

	/**
	 * Set the decimal scale.
	 * @param scale the decimal scale
	 */
	public void setScale(Integer scale) {
		this.scale = scale;
	}

	/**
	 * Convert the schema into the actual object.
	 * @param text in the form of DECIMAL(p, s)
	 * @return the corresponding AvroDecimal
	 */
	@JsonIgnore
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
	 * creates the datatype from schema
	 * @param schema with the decimal details
	 * @return the corresponding AvroDecimal
	 */
	public static AvroDecimal create(Schema schema) {
		return new AvroDecimal(schema);
	}

	/**
	 * create from the logical type
	 * @param l based on this Avro native logical type
	 * @return the corresponding AvroDecimal
	 */
	public static AvroDecimal create(Decimal l) {
		return new AvroDecimal(l);
	}

	/**
	 * create decimal with the given precision and scale
	 * @param precision number of digits the decimal can hold
	 * @param scale number of digits used for the scale
	 * @return an AvroDecimal with the provided precision and scale
	 */
	public static AvroDecimal create(int precision, int scale) {
		return new AvroDecimal(precision, scale);
	}

	/**
	 * create based on the textual representation of the data type
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
		this.scale = scale;
		this.precision = precision;
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
		this.scale = decimal.getScale();
		this.precision = decimal.getPrecision();
		this.schema = schema;
	}

	/**
	 * Constructor for this static instance
	 * @param l based on this Avro native logical type
	 */
	private AvroDecimal(Decimal l) {
		super(NAME);
		decimal = l;
		this.scale = decimal.getScale();
		this.precision = decimal.getPrecision();
		this.schema = l.addToSchema(Schema.create(Type.BYTES));
	}

	/**
	 * creates the schema of this datatype.
	 * @return the static schema of this type
	 */
	public Schema createSchema() {
		return schema;
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
		if (o == null) {
			return false;
		} else if (o instanceof AvroDecimal d) {
			return Objects.equals(this.precision, d.precision) && Objects.equals(this.scale, d.scale);
		} else {
			return false;
		}
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

	/**
	 * Factory class to create an instance of this logical type
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor to register this factory
		 */
		public Factory() {
		}

		@Override
		/**
		 * creates the LogicalType fromSchema operation.
		 * @param schema the parameter value
		 */
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

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		BigDecimal b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
