/**
 * 
 */
package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.kafka.avro.SchemaConstants;

import org.apache.avro.SchemaBuilderException;

/**
 * A class that helps creating an AvroSchema by code for the Key record.
 *
 */
public class KeySchema extends SchemaBuilder {

	/**
	 * @param name of the key schema
	 * @param namespace optional namespace identifier
	 * @param description free form text
	 */
	public KeySchema(String name, String namespace, String description) {
		super(name, namespace, description);
	}

	/**
	 * @param name of the key schema
	 * @param description free form text
	 */
	public KeySchema(String name, String description) {
		super(name, description);
	}
	
	/**
	 * Derive the key schema from the value schema using its primary key flags
	 * 
	 * @param valueschema the key schema is based on
	 * @return KeySchema
	 * @throws SchemaBuilderException if the value schema is invalid
	 */
	public static Schema create(Schema valueschema) throws SchemaBuilderException {
		KeySchema kbuilder = new KeySchema(valueschema.getName(), valueschema.getNamespace(), valueschema.getDoc());
		int count = 0;
		for (Field f : valueschema.getFields()) {
			if (AvroField.isPrimaryKey(f)) {
				kbuilder.add(f);
				count++;
			}
		}
		if (count == 0) {
			kbuilder.add(valueschema.getField(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM));
			kbuilder.add(valueschema.getField(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION));
			kbuilder.add(valueschema.getField(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID));
		}
		kbuilder.build();
		return kbuilder.getSchema();
	}

}
