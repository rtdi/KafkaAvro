/**
 *
 */
package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroField;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;

import org.apache.avro.SchemaBuilderException;

/**
 * A class that helps creating an AvroSchema by code for the Key record.
 *
 */
public class KeySchema extends RecordSchema {

	/**
	 * @param name of the key schema
	 * @param namespace optional namespace identifier
	 * @param description free form text
	 */
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param namespace the parameter value
	 * @param description the parameter value
	 */
	public KeySchema(String name, String namespace, String description) {
		super(name, namespace, description);
	}

	/**
	 * @param name of the key schema
	 * @param description free form text
	 */
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param description the parameter value
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
	/**
	 * Executes the Schema create operation and returns the resulting value.
	 * @param valueschema the parameter value
	 * @return the resulting value
	 */
	public static Schema create(ValueSchema valueschema) throws SchemaBuilderException {
		KeySchema kbuilder = new KeySchema(valueschema.getName(), valueschema.getNamespace(), valueschema.getDoc());
		List<String> pks = valueschema.getPrimaryKeys();
		if (pks == null || pks.size() == 0) {
			kbuilder.add(valueschema.getField(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM));
			kbuilder.add(valueschema.getField(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION));
			kbuilder.add(valueschema.getField(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID));
		} else {
			for (String pk : pks) {
				AvroField f = valueschema.getField(pk);
				if (f == null) {
					throw new SchemaBuilderException("The value schema does not contain a field \"" + pk + "\" in the root schema");
				}
				kbuilder.add(f);
			}
		}
		return kbuilder.createSchema();
	}

}
