package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;


public class AvroRecordArray extends AvroRecordAbstract {

	public AvroRecordArray(String name, SchemaBuilder schema, String doc, Object defaultValue, Order order) throws SchemaBuilderException {
		super(name, Schema.createArray(schema.getSchema()), doc, defaultValue, true, order, schema);
	}

	public AvroRecordArray(String name, SchemaBuilder schema, String doc, Object defaultValue) throws SchemaBuilderException {
		super(name, Schema.createArray(schema.getSchema()), doc, true, defaultValue, schema);
	}

	public SchemaBuilder getArrayElementSchemaBuilder() {
		return schemabuilder;
	}
		
}
