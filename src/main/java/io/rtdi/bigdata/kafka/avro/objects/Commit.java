package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroBoolean;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroLong;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class Commit {

    public static ValueSchema commit_schema = new ValueSchema("commit", "A commit for a function to be called when certain events occur");
    static {
        RecordSchema offset = new RecordSchema("min_max_offsets", "the min/max offsets for a partition");
        offset.add("min_offset", AvroLong.create(), "the minimum offset for this partition", false);
        offset.add("max_offset", AvroLong.create(), "the maximum offset for this partition", false);
        offset.add("partition", AvroInt.create(), "the partition number", false);

        RecordSchema topics = new RecordSchema("topic_offsets", "the topic offsets for a commit");
        topics.add("topic_name", AvroString.create(), "the topic name", false);
        topics.add("schema_names", AvroArray.create(AvroString.create()), "the schema names for this topic", false);
        topics.add("offsets", new AvroMap(offset), "the offsets for this topic", false);

        commit_schema.add("commit_id", AvroString.create(), "the commit id", false);
        commit_schema.add("producer_name", AvroString.create(), "the producer name", false);
        commit_schema.add("commit_epoch_ns", AvroLong.create(), "the epoch timestamp in nanoseconds when the commit was issued", false);
        commit_schema.add("record_count", AvroInt.create(), "the number of records in this commit", true);
        commit_schema.add("rollback", AvroBoolean.create(), "if true, this commit is a rollback of this transaction", true);
        commit_schema.add("topics", new AvroMap(topics), "the topics and their offsets for this commit", true);
        commit_schema.setPrimaryKeys("commit_id", "producer_name");
    }

    public String commit_id;
    public String producer_name;
    public long commit_epoch_ns;
    public Integer record_count;
    public Boolean rollback;
    public java.util.Map<String, TopicOffsets> topics;


    public static class TopicOffsets {
        public String topic_name;
        public java.util.List<String> schema_names;
        public java.util.Map<String, MinMaxOffsets> offsets;
    }

    public static class MinMaxOffsets {
        public long min_offset;
        public long max_offset;
        public int partition;
    }

}

