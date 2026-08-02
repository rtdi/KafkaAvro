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
	 * get the name of the logical type
	 * @return the name of the logical type
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the logical type.
	 * @param name the parameter value
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * get the namespace of the logical type
	 * @return the namespace of the logical type
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Sets the namespace of the logical type.
	 * @param namespace the parameter value
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/**
	 * get the documentation of the logical type
	 * @return the documentation of the logical type
	 */
	public String getDoc() {
		return doc;
	}

	/**
	 * Sets the documentation of the logical type.
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
	public static AvroFixed create(String name, String namespace, int length, String doc) {
		return new AvroFixed(name, namespace, length, doc);
	}

	/**
	 * Create the logical type from the schema. The schema must be of type FIXED.
	 *
	 * @param schema to create the logical type from
	 * @return the logical type
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
	 * Create a new data type with this length
	 * @param length in bytes of the fixed-length binary data type
	 * @return An AvroFixed data type with name FIXEDnnnn where nnnn is the length
	 */
	public static AvroFixed create(int length) {
		return AvroFixed.create("FIXED" + length, null, length, null);
	}

	/**
	 * Get the schema that describes this logical type
	 * 
	 * @param name of the fixed schema
	 * @param namespace of the fixed schema
	 * @param length of this data type
	 * @param doc description
	 * @return the corresponding schema
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
	public static Schema getSchema(int length) {
		return create(length).createSchema();
	}

	/**
	 * Get the schema that describes this logical type
	 *
	 * @return the schema
	 */
	public Schema createSchema() {
		if (schema == null) {
			schema = addToSchema(Schema.createFixed(name, doc, namespace, getLength()));
		}
		return schema;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.FIXED) {
			throw new IllegalArgumentException("Logical type must be backed by a FIXED");
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
		public Factory() {
		}

		@Override
		/**
		 * create type from schema.
		 * @param schema the parameter value
		 */
		public LogicalType fromSchema(Schema schema) {
			return AvroFixed.create(schema);
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
		return Type.FIXED;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROFIXED;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException {
		byte[] b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			return "\"" + Base64.getEncoder().encodeToString(b) + "\"";
		}
	}

}
