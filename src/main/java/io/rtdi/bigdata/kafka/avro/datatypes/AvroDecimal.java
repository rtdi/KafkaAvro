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
	 * @param precision number of digits the decimal can hold
	 * @param scale number of digits used for the scale
	 * @return the schema used for this data type
	 */
	/**
	 * Executes the Schema getSchema operation and returns the resulting value.
	 * @param precision the parameter value
	 * @param scale the parameter value
	 * @return the resulting value
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
	 * Executes the Integer getPrecision operation.
	 */
	public Integer getPrecision() {
		return precision;
	}

	/**
	 * Executes the void setPrecision operation.
	 * @param precision the parameter value
	 */
	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	/**
	 * Executes the Integer getScale operation.
	 */
	public Integer getScale() {
		return scale;
	}

	/**
	 * Executes the void setScale operation.
	 * @param scale the parameter value
	 */
	public void setScale(Integer scale) {
		this.scale = scale;
	}

	/**
	 * @param text in the form of DECIMAL(p, s)
	 * @return the corresponding AvroDecimal
	 */
	@JsonIgnore
	/**
	 * Executes the Schema getSchema operation and returns the resulting value.
	 * @param text the parameter value
	 * @return the resulting value
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
	/**
	 * Executes the AvroDecimal create operation and returns the resulting value.
	 * @param schema the parameter value
	 * @return the resulting value
	 */
	public static AvroDecimal create(Schema schema) {
		return new AvroDecimal(schema);
	}

	/**
	 * @param l based on this Avro native logical type
	 * @return the corresponding AvroDecimal
	 */
	/**
	 * Executes the AvroDecimal create operation and returns the resulting value.
	 * @param l the parameter value
	 * @return the resulting value
	 */
	public static AvroDecimal create(Decimal l) {
		return new AvroDecimal(l);
	}

	/**
	 * @param precision number of digits the decimal can hold
	 * @param scale number of digits used for the scale
	 * @return an AvroDecimal with the provided precision and scale
	 */
	/**
	 * Executes the AvroDecimal create operation and returns the resulting value.
	 * @param precision the parameter value
	 * @param scale the parameter value
	 * @return the resulting value
	 */
	public static AvroDecimal create(int precision, int scale) {
		return new AvroDecimal(precision, scale);
	}

	/**
	 * @param text containing the data type definition as text in the form of DECIMAL(p, s)
	 * @return the corresponding AvroDecimal
	 */
	/**
	 * Executes the AvroDecimal create operation and returns the resulting value.
	 * @param text the parameter value
	 * @return the resulting value
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
	 * @return the static schema of this type
	 */
	/**
	 * Executes the Schema createSchema operation.
	 */
	public Schema createSchema() {
		return schema;
	}

	@Override
	/**
	 * Executes the Schema addToSchema operation.
	 * @param schema the parameter value
	 */
	public Schema addToSchema(Schema schema) {
		return decimal.addToSchema(schema);
	}

	@Override
	/**
	 * Executes the void validate operation.
	 * @param schema the parameter value
	 */
	public void validate(Schema schema) {
		decimal.validate(schema);
	}

	@Override
	/**
	 * Executes the boolean equals operation.
	 * @param o the parameter value
	 */
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
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return decimal.hashCode();
	}

	@Override
	/**
	 * Executes the String toString operation.
	 */
	public String toString() {
		return NAME + "(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
	}

	@Override
	/**
	 * Executes the Object convertToInternal operation.
	 * @param value the parameter value
	 */
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
			return AvroDecimal.create(schema);
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
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.BYTES;
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
		return AvroType.AVRODECIMAL;
	}

	@Override
	/**
	 * Executes the BigDecimal convertToJava operation.
	 * @param value the parameter value
	 */
	public BigDecimal convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof ByteBuffer) {
			return DECIMAL_CONVERTER.fromBytes((ByteBuffer) value, null, decimal);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Decimal");
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		BigDecimal b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + b.toString() + "\"";
		}
	}

}
