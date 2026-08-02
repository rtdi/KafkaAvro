package io.rtdi.bigdata.kafka.avro.objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroBoolean;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroLong;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * Schema for a commit message, confirming that a table was loaded with 0..n rows.
 */
public class Commit {

    /**
     * The schema for a commit message.
     */
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

    private String commit_id;
    private String producer_name;
    private long commit_epoch_ns;
    private Integer record_count;
    private Boolean rollback;
    private java.util.Map<String, TopicOffsets> topics;

    /**
     * Creates a new instance of this class.
     */
    public Commit() {
    }

    /**
     * Creates a new instance of this class.
     * @param commit_id the commit id
     * @param producer_name the producer name
     * @param commit_epoch_ns the commit epoch in nanoseconds
     * @param record_count the record count
     * @param rollback the rollback flag
     */
    public Commit(String commit_id, String producer_name, long commit_epoch_ns, Integer record_count, Boolean rollback) {
        this.commit_id = commit_id;
        this.producer_name = producer_name;
        this.commit_epoch_ns = commit_epoch_ns;
        this.record_count = record_count;
        this.rollback = rollback;
    }

    /**
     * Gets the commit id.
     * @return the commit id
     */
    @JsonProperty("commit_id")
    public String getCommit_id() {
        return commit_id;
    }

    /**
     * Sets the commit id.
     * @param commit_id the commit id to set
     */
    public void setCommit_id(String commit_id) {
        this.commit_id = commit_id;
    }

    /**
     * Gets the producer name.
     * @return the producer name
     */
    public String getProducer_name() {
        return producer_name;
    }
    /**
     * Sets the producer name.
     * @param producer_name the producer name to set
     */
    public void setProducer_name(String producer_name) {
        this.producer_name = producer_name;
    }

    /**
     * Gets the commit epoch in nanoseconds.
     * @return the commit epoch in nanoseconds
     */
    @JsonProperty("commit_epoch_ns")
    public long getCommit_epoch_ns() {
        return commit_epoch_ns;
    }

    /**
     * Sets the commit epoch in nanoseconds.
     * @param commit_epoch_ns the commit epoch in nanoseconds to set
     */
    public void setCommit_epoch_ns(long commit_epoch_ns) {
        this.commit_epoch_ns = commit_epoch_ns;
    }

    /**
     * Gets the record count.
     * @return the record count
     */
    @JsonProperty("record_count")
    public Integer getRecord_count() {
        return record_count;
    }

    /**
     * Sets the record count.
     * @param record_count the record count to set
     */
    public void setRecord_count(Integer record_count) {
        this.record_count = record_count;
    }

    /**
     * Gets the rollback flag.
     * @return if true this message is a rollback of this transaction
     */
    @JsonProperty("rollback")
    public Boolean getRollback() {
        return rollback;
    }

    /**
     * Sets the rollback flag.
     * @param rollback the rollback flag to set
     */
    public void setRollback(Boolean rollback) {
        this.rollback = rollback;
    }

    /**
     * Gets the topics and their offsets for this commit.
     * @return the topics and their offsets
     */
    @JsonProperty("topics")
    public java.util.Map<String, TopicOffsets> getTopics() {
        return topics;
    }

    /**
     * Sets the topics and their offsets for this commit.
     * @param topics the topics and their offsets to set
     */
    public void setTopics(java.util.Map<String, TopicOffsets> topics) {
        this.topics = topics;
    }



    /**
     * For each topic the list of schema names loaded and the min/max offsets for each partition is stored.
     */
    public static class TopicOffsets {
        private String topic_name;
        private java.util.List<String> schema_names;
        private java.util.Map<String, MinMaxOffsets> offsets;

        /**
         * Creates a new instance of this class.
         */
        public TopicOffsets() {
        }

        /**
         * Creates a new instance of this class.
         * @param topic_name the parameter value
         */
        public TopicOffsets(String topic_name) {
            this.topic_name = topic_name;
        }

        /**
         * Gets the topic name.
         * @return the topic name
         */
        @JsonProperty("topicName")
        public String getTopic_name() {
            return topic_name;
        }

        /**
         * Sets the topic name.
         * @param topic_name the topic name to set
         */
        public void setTopicName(String topic_name) {
            this.topic_name = topic_name;
        }

        /**
         * Gets the list of schema names loaded for this topic.
         * @return the list of schema names
         */
        @JsonProperty("schema_names")
        public java.util.List<String> getSchemaNames() {
            return schema_names;
        }

        /**
         * Sets the list of schema names loaded for this topic.
         * @param schema_names the list of schema names to set
         */
        public void setSchemaNames(java.util.List<String> schema_names) {
            this.schema_names = schema_names;
        }

        /**
         * Gets the offsets for this topic.
         * @return the offsets
         */
        @JsonProperty("offsets")
        public java.util.Map<String, MinMaxOffsets> getOffsets() {
            return offsets;
        }

        /**
         * Sets the offsets for this topic.
         * @param offsets the offsets to set
         */
        public void setOffsets(java.util.Map<String, MinMaxOffsets> offsets) {
            this.offsets = offsets;
        }

        
    }

    /**
     * Represents the minimum and maximum offsets for a partition.
     */
    public static class MinMaxOffsets {
        private long min_offset;
        private long max_offset;
        private int partition;

        /**
         * Creates a new instance of this class.
         */
        public MinMaxOffsets() {
        }

        /**
         * Creates a new instance of this class.
         * @param min_offset the minimum offset
         * @param max_offset the maximum offset
         * @param partition the partition
         */
        public MinMaxOffsets(long min_offset, long max_offset, int partition) {
            this.min_offset = min_offset;
            this.max_offset = max_offset;
            this.partition = partition;
        }

        /**
         * Gets the minimum offset.
         * @return the minimum offset
         */
        public long getMin_offset() {
            return min_offset;
        }

        /**
         * Sets the minimum offset.
         * @param min_offset the minimum offset to set
         */
        public void setMin_offset(long min_offset) {
            this.min_offset = min_offset;
        }

        /**
         * Gets the maximum offset.
         * @return the maximum offset
         */
        public long getMax_offset() {
            return max_offset;
        }

        /**
         * Sets the maximum offset.
         * @param max_offset the maximum offset to set
         */
        public void setMax_offset(long max_offset) {
            this.max_offset = max_offset;
        }

        /**
         * Gets the partition.
         * @return the partition
         */
        public int getPartition() {
            return partition;
        }

        /**
         * Sets the partition.
         * @param partition the partition to set
         */
        public void setPartition(int partition) {
            this.partition = partition;
        }

        
    }

}

