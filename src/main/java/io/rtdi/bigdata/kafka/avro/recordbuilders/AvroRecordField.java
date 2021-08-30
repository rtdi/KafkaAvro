package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.SchemaBuilderException;

public class AvroRecordField extends AvroRecordAbstract {

	/**
	 * @param name of the field
	 * @param schema of the field
	 * @param doc description
	 * @param nullable true if the field is optional
	 * @param defaultValue an optional default value
	 * @param order of the Avro field
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroRecordField(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue, Order order) throws SchemaBuilderException {
		super(name, schema.getSchema(), doc, defaultValue, nullable, order, schema);
	}

	/**
	 * @param name of the field
	 * @param schema of the field
	 * @param doc description
	 * @param nullable true if the field is optional
	 * @param defaultValue an optional default value
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroRecordField(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue) throws SchemaBuilderException {
		super(name, schema.getSchema(), doc, nullable, defaultValue, schema);
	}

	/**
	 * @return the schema builder for this record
	 */
	public SchemaBuilder getSchemaBuilder() {
		return schemabuilder;
	}
	
}
