package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.Arrays;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.EnumSymbol;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroNameEncoder;

/**
 * Wrapper around the Avro Type.ENUM data type
 *
 */
public class AvroEnum extends AvroLogicalType implements IAvroPrimitive {
	/**
	 * Factory instance to be registered with Avro
	 */
	public static final Factory factory = new Factory();
	/**
	 * Name of the logical type
	 */
	public static final String NAME = "ENUM";
	private Schema schema;
	private String[] symbols;
	private String name;
	private String namespace;

	/**
	 * Creates the Avro schema backing this logical enum type.
	 *
	 * @return the enum schema
	 */
	public Schema createSchema() {
		return schema;
	}

	/**
	 * Gets the allowed symbols for the enum.
	 *
	 * @return the enum symbols
	 */
	public String[] getSymbols() {
		return symbols;
	}

	/**
	 * Sets the allowed symbols for the enum.
	 *
	 * @param symbols the enum symbols
	 */
	public void setSymbols(String[] symbols) {
		this.symbols = symbols;
	}

	/**
	 * Gets the logical enum name.
	 *
	 * @return the enum name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the logical enum name.
	 *
	 * @param name the enum name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the namespace associated with the enum schema.
	 *
	 * @return the namespace
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Sets the namespace associated with the enum schema.
	 *
	 * @param namespace the namespace
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/**
	 * Creates an Avro enum schema from a Java enum class.
	 *
	 * @param <T> the enum type
	 * @param symbols the enum class
	 * @return the enum schema with this logical type
	 */
	public static <T extends Enum<T>> Schema getSchema(Class<T> symbols) {
		return create(symbols).createSchema();
	}

	/**
	 * Constructs a logical enum type backed by an Avro enum schema.
	 *
	 * @param name the enum name
	 * @param namespace the enum namespace
	 * @param symbols the allowed enum symbols
	 * @param doc the schema documentation text
	 */
	private AvroEnum(String name, String namespace, String[] symbols, String doc) {
		super(NAME);
		this.name = name;
		this.namespace = namespace;
		this.symbols = symbols;
		this.schema = this.addToSchema(Schema.createEnum(AvroNameEncoder.encodeName(name), doc, namespace, Arrays.asList(symbols)));
	}

	private AvroEnum() {
		super(NAME);
	}

	/**
	 * Create an instance of that type.
	 * @param name of the enum
	 * @param namespace of the enum
	 * @param symbols list of allowed values
	 * @param doc of the enum
	 * @return the instance
	 */
	public static AvroEnum create(String name, String namespace, String[] symbols, String doc) {
		return new AvroEnum(name, namespace, symbols, doc);
	}

	/**
	 * Creates a logical enum type from a Java enum class.
	 *
	 * @param <T> the enum type
	 * @param symbols the enum class
	 * @return the logical enum type
	 */
	public static <T extends Enum<T>> AvroEnum create(Class<T> symbols) {
		String[] names = new String[symbols.getEnumConstants().length];
		int i = 0;
		for (T t : symbols.getEnumConstants()) {
			names[i] = t.name();
			i++;
		}
		return create(symbols.getSimpleName(), null, names, null);
	}

	/**
	 * Create an instance of that type.
	 * @param name of the enum
	 * @param symbols list of allowed values
	 * @param doc of the enum
	 * @return the instance
	 */
	public static AvroEnum create(String name, String[] symbols, String doc) {
		return create(name, null, symbols, doc);
	}

	/**
	 * Create this logical type based on an Enum schema
	 *
	 * @param schema of the enum
	 * @return instance of the AvroEnum
	 */
	public static AvroEnum create(Schema schema) {
		AvroEnum element = new AvroEnum(schema.getName(), schema.getNamespace(), schema.getEnumSymbols().toArray(new String[0]), schema.getDoc());
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.ENUM) {
			throw new IllegalArgumentException("Logical type must be backed by a ENUM");
		}
	}

	@Override
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
	public int hashCode() {
		return 1;
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public EnumSymbol convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof Enum) {
			return new EnumSymbol(schema, ((Enum<?>) value).name());
		} else {
			return new EnumSymbol(schema, value);
		}
	}

	@Override
	public String convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof EnumSymbol) {
			return ((EnumSymbol) value).toString();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Enum");
	}

	/**
	 * Factory to create this logical type.
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Creates a factory that registers the logical enum type with Avro.
		 */
		public Factory() {
		}

		@Override
		/**
		 * creates the LogicalType fromSchema operation.
		 * @param schema the parameter value
		 */
		public LogicalType fromSchema(Schema schema) {
			return AvroEnum.create(schema);
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append('\"');
			b.append(value.toString());
			b.append('\"');
		}
	}

	@Override
	public Type getBackingType() {
		return Type.ENUM;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROENUM;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		if (value == null) {
			return "null";
		} else {
			return "\"" + value + "\"";
		}
	}

}
