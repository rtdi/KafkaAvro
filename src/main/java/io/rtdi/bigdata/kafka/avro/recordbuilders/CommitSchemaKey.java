package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;

public class CommitSchemaKey extends SchemaBuilder {
	public static final CommitSchemaKey COMMIT_SCHEMA_KEY = new CommitSchemaKey();

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
