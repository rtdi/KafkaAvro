package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;


/**
 * Array record
 */
public class AvroRecordArray extends AvroRecordAbstract {

	/**
	 * @param name of the field
	 * @param schema of the field
	 * @param doc description
	 * @param defaultValue an optional default value
	 * @param order of the Avro field
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroRecordArray(String name, SchemaBuilder schema, String doc, Object defaultValue, Order order) throws SchemaBuilderException {
		super(name, Schema.createArray(schema.getSchema()), doc, defaultValue, true, order, schema);
	}

	/**
	 * @param name of the field
	 * @param schema of the field
	 * @param doc description
	 * @param defaultValue an optional default value
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroRecordArray(String name, SchemaBuilder schema, String doc, Object defaultValue) throws SchemaBuilderException {
		super(name, Schema.createArray(schema.getSchema()), doc, true, defaultValue, schema);
	}

	/**
	 * @return the schema builder for this array
	 */
	public SchemaBuilder getArrayElementSchemaBuilder() {
		return schemabuilder;
	}

}
