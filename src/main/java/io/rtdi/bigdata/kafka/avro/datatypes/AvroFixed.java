package io.rtdi.bigdata.kafka.avro.datatypes;

import java.nio.ByteBuffer;
import java.util.Base64;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Fixed;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * Wrapper around the Avro Type.Fixed data type
 *
 */
public class AvroFixed extends LogicalTypeWithLength {
	/**
	 * Factory instance to be registered with Avro
	 */
	public static final Factory factory = new Factory();
	/**
	 * Name of this data type in Avro schema
	 */
	public static final String NAME = "FIXED";
	private Schema schema;
	private String name;
	private String namespace;
	private String doc;

	/**
	 * @param length of this data type
	 */
	private AvroFixed(String name, String namespace, int length, String doc) {
		super(NAME, length);
		this.name = name;
		this.namespace = namespace;
		this.doc = doc;
		this.schema = addToSchema(Schema.createFixed(name, doc, namespace, length));
	}

	/**
	 * Creates a new instance of this class.
	 */
	public AvroFixed() {
		super(NAME);
	}

	/**
	 * Executes the String getName operation.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Executes the void setName operation.
	 * @param name the parameter value
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Executes the String getNamespace operation.
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Executes the void setNamespace operation.
	 * @param namespace the parameter value
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/**
	 * Executes the String getDoc operation.
	 */
	public String getDoc() {
		return doc;
	}

	/**
	 * Executes the void setDoc operation.
	 * @param doc the parameter value
	 */
	public void setDoc(String doc) {
		this.doc = doc;
	}

	/**
	 * Create a new data type with this length
	 *
	 * @param name of the fixed schema
	 * @param namespace of the fixed schema
	 * @param length of this data type
	 * @param doc description
	 * @return a new data type with this length
	 */
	/**
	 * Executes the AvroFixed create operation and returns the resulting value.
	 * @param name the parameter value
	 * @param namespace the parameter value
	 * @param length the parameter value
	 * @param doc the parameter value
	 * @return the resulting value
	 */
	public static AvroFixed create(String name, String namespace, int length, String doc) {
		return new AvroFixed(name, namespace, length, doc);
	}

	/**
	 * Create the logical type from the schema. The schema must be of type FIXED.
	 *
	 * @param schema to create the logical type from
	 * @return the logical type
	 */
	/**
	 * Executes the AvroFixed create operation and returns the resulting value.
	 * @param schema the parameter value
	 * @return the resulting value
	 */
	public static AvroFixed create(Schema schema) {
		AvroFixed element = new AvroFixed(
			schema.getName(),
			schema.getNamespace(),
			schema.getFixedSize(),
			schema.getDoc()
		);
		element.schema = schema;
		return element;
	}

	/**
	 * @param length in bytes of the fixed-length binary data type
	 * @return An AvroFixed data type with name FIXEDnnnn where nnnn is the length
	 */
	/**
	 * Executes the AvroFixed create operation and returns the resulting value.
	 * @param length the parameter value
	 * @return the resulting value
	 */
	public static AvroFixed create(int length) {
		return AvroFixed.create("FIXED" + length, null, length, null);
	}

	/**
	 * @param name of the fixed schema
	 * @param namespace of the fixed schema
	 * @param length of this data type
	 * @param doc description
	 * @return the corresponding schema
	 */
	/**
	 * Executes the Schema getSchema operation and returns the resulting value.
	 * @param name the parameter value
	 * @param namespace the parameter value
	 * @param length the parameter value
	 * @param doc the parameter value
	 * @return the resulting value
	 */
	public static Schema getSchema(String name, String namespace, int length, String doc) {
		return create(name, namespace, length, doc).createSchema();
	}

	/**
	 * Get the schema that describes this logical type
	 *
	 * @param length in bytes of the fixed-length binary data type
	 * @return An AvroFixed schema with name FIXEDnnnn where nnnn is the length
	 */
	/**
	 * Executes the Schema getSchema operation and returns the resulting value.
	 * @param length the parameter value
	 * @return the resulting value
	 */
	public static Schema getSchema(int length) {
		return create(length).createSchema();
	}

	/**
	 * Get the schema that describes this logical type
	 *
	 * @return the schema
	 */
	/**
	 * Executes the Schema createSchema operation.
	 */
	public Schema createSchema() {
		if (schema == null) {
			schema = addToSchema(Schema.createFixed(name, doc, namespace, getLength()));
		}
		return schema;
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
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.FIXED) {
			throw new IllegalArgumentException("Logical type must be backed by a FIXED");
		}
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
		return 1;
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
	 * Executes the Fixed convertToInternal operation.
	 * @param value the parameter value
	 */
	public Fixed convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof ByteBuffer) {
			return new Fixed(schema, ((ByteBuffer) value).array());
		} else if (value instanceof byte[]) {
			return new Fixed(schema, (byte[]) value);
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a ByteBuffer");
	}

	@Override
	/**
	 * Executes the byte[] convertToJava operation.
	 * @param value the parameter value
	 */
	public byte[] convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof byte[]) {
			return (byte[]) value;
		} else if (value instanceof ByteBuffer) {
			return ((ByteBuffer) value).array();
		} else if (value instanceof Fixed) {
			return ((Fixed) value).bytes();
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Fixed");
	}

	/**
	 * Factory class to create this logical type from a schema
	 */
	public static class Factory implements LogicalTypeFactory {

		/**
		 * Constructor to be used by Avro when the factory is registered
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
			return AvroFixed.create(schema);
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
			b.append('\"');
			b.append(value.toString());
			b.append('\"');
		}
	}

	@Override
	/**
	 * Executes the Type getBackingType operation.
	 */
	public Type getBackingType() {
		return Type.FIXED;
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
		return AvroType.AVROFIXED;
	}

	@Override
	/**
	 * Executes the String convertToJson operation.
	 * @param value the parameter value
	 */
	public String convertToJson(Object value) throws AvroDataTypeException {
		byte[] b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + Base64.getEncoder().encodeToString(b) + "\"";
		}
	}

}
