package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;

/**
 * Commit schema key.
 */
public class CommitSchemaKey extends SchemaBuilder {
	/**
	 * The singleton instance of the CommitSchemaKey
	 */
	public static final CommitSchemaKey COMMIT_SCHEMA_KEY = new CommitSchemaKey();

	/**
	 * Get the singleton instance of the CommitSchemaKey
	 *
	 * @return the instance
	 */
	public static CommitSchemaKey getInstance() {
		return COMMIT_SCHEMA_KEY;
	}

	private CommitSchemaKey() {
		super("commit-key", null);
		add("commit_id", Schema.create(Schema.Type.STRING), null, false);
		add("producer_name", Schema.create(Schema.Type.STRING), null, false);
		build();
	}

}
