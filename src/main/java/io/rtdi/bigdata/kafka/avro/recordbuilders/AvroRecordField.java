package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.SchemaBuilderException;

public class AvroRecordField extends AvroRecordAbstract {

	public AvroRecordField(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue, Order order) throws SchemaBuilderException {
		super(name, schema.getSchema(), doc, defaultValue, nullable, order, schema);
	}

	public AvroRecordField(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue) throws SchemaBuilderException {
		super(name, schema.getSchema(), doc, nullable, defaultValue, schema);
	}

	public SchemaBuilder getSchemaBuilder() {
		return schemabuilder;
	}
	
}
