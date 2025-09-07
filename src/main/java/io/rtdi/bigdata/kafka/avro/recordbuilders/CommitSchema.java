package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;

public class CommitSchema extends SchemaBuilder {
	public static final CommitSchema COMMIT_SCHEMA = new CommitSchema();

	public static CommitSchema getInstance() {
		return COMMIT_SCHEMA;
	}

	private CommitSchema() {
		super("commit", null);
		add("commit_id", Schema.create(Schema.Type.STRING), null, false);
		add("producer_name", Schema.create(Schema.Type.STRING), null, false);
		add("commit_epoch_ns", Schema.create(Schema.Type.LONG), null, false);
		add("record_count", Schema.create(Schema.Type.INT), null, false, 0);
		addProp(ValueSchema.PRIMARY_KEYS, Arrays.asList("commit_id", "producer_name"));
		Schema topicoffsetsschema;
		Schema minmaxoffsetsschema;
		Schema minmaxoffsetsrecord = Schema.createRecord("min_max_offsets", null, null, false);
		minmaxoffsetsrecord.setFields(
				List.of(
						new Schema.Field("min_offset", Schema.create(Schema.Type.LONG), null, (Object) null),
						new Schema.Field("max_offset", Schema.create(Schema.Type.LONG), null, (Object) null),
						new Schema.Field("partition", Schema.create(Schema.Type.INT), null, (Object) null)
						));
		minmaxoffsetsschema = Schema.createMap(minmaxoffsetsrecord);
		Schema offsetsarray = Schema.createArray(minmaxoffsetsschema);
		Schema topicoffsetsrecord = Schema.createRecord("topic_offsets", null, null, false);
		topicoffsetsrecord.setFields(
				List.of(
						new Schema.Field("topic_name", Schema.create(Schema.Type.STRING), null, (Object) null),
						new Schema.Field("schema_names", Schema.createArray(Schema.create(Schema.Type.STRING)), null, (Object) null),
						new Schema.Field("offsets", offsetsarray, null, (Object) null)
						));
		topicoffsetsschema = topicoffsetsrecord;
		Schema topicschema = Schema.createMap(topicoffsetsschema);
		add("topics", AvroMap.getSchema(topicschema), null, true);
		build();
	}

}
