package io.rtdi.bigdata.kafka.avro.objects;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
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
     * The value schema for a commit message.
     */
    public static ValueSchema commit_schema = new ValueSchema("commit", "A commit for a function to be called when certain events occur");
    /**
     * The Avro schema for a commit message.
     */
    public static final Schema avro_schema;
    private static final Schema avro_schema_topics;
    private static final Schema avro_schema_offset;
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
        avro_schema = commit_schema.createSchema();
        avro_schema_topics = AvroUtils.getBaseSchema(avro_schema.getField("topics").schema()).getValueType();
        avro_schema_offset = AvroUtils.getBaseSchema(avro_schema_topics.getField("offsets").schema()).getValueType();
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
     * Get the current object as GenericRecord
     * @return GenericRecord
     */
    public GenericData.Record toRecord() {
        GenericData.Record r = new GenericData.Record(avro_schema);
        r.put("topics", TopicOffsets.create(topics));
        r.put("commit_id", this.commit_id);
        r.put("producer_name", this.producer_name);
        r.put("commit_epoch_ns", this.commit_epoch_ns);
        r.put("record_count", this.record_count);
        r.put("rollback", this.rollback);
        return r;
    }

    /**
     * Create a ImpactLineage instance from the Avro record data
     * 
     * @param data Avro record
     * @return the ImpactLineage instance
     * @throws AvroTypeException in case the record does not match the structure
     */
    public static Commit from(GenericData.Record data) throws AvroTypeException {
        Commit t = new Commit();
        t.setCommit_id(AvroUtils.getAvroValue(data, "commit_id", String.class));
        t.setProducer_name(AvroUtils.getAvroValue(data, "producer_name", String.class));
        t.setCommit_epoch_ns(AvroUtils.getAvroValue(data, "commit_epoch_ns", Long.class));
        t.setRecord_count(AvroUtils.getAvroValue(data, "record_count", Integer.class));
        t.setRollback(AvroUtils.getAvroValue(data, "rollback", Boolean.class));
        t.setTopics(TopicOffsets.from(AvroUtils.getAvroMapOfRecords(data, "topics")));
        return t;
    }
    
    /**
     * Serialize the record values into Json
     * @return the Trigger values as string
     * @throws JsonProcessingException in case the object cannot be serialized
     */
    public String toRecordJson() throws JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.writeValueAsString(this);
    }

	/**
	 * Deserializes a JSON payload into a {@link Commit} instance.
	 *
	 * @param json the JSON payload
	 * @return the parsed value schema
	 * @throws JsonMappingException if the JSON structure cannot be mapped
	 * @throws JsonProcessingException if the JSON payload cannot be read
	 */
	public static Commit fromRecordJson(String json) throws JsonMappingException, JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.readValue(json, Commit.class);
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
     * Returns a hash code based on the parent function name.
     *
     * @return the hash code for this event set
     */
    @Override
    public int hashCode() { return Objects.hash(this.commit_id); }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof Commit t) {
            return Objects.equals(this.commit_id, t.commit_id) && 
                Objects.equals(this.producer_name, t.producer_name) && 
                Objects.equals(this.record_count, t.record_count) && 
                Objects.equals(this.rollback, t.rollback) &&
                Objects.equals(this.commit_epoch_ns, t.commit_epoch_ns) &&
                AvroUtils.isEqual(this.topics, t.topics);
        } else {
            return false;
        }
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

        private static Map<String, TopicOffsets> from(Map<String, GenericData.Record> data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                Map<String, TopicOffsets> m = new HashMap<>();
                for(Entry<String, GenericData.Record> e : data.entrySet()) {
                    m.put(e.getKey(), TopicOffsets.from(e.getValue()));
                }
                return m;
            }
        }

        private static TopicOffsets from(GenericData.Record data) {
            TopicOffsets t = new TopicOffsets();
            t.setTopicName(AvroUtils.getAvroValue(data, "topic_name", String.class));
            t.setSchemaNames(AvroUtils.getAvroListOfString(data, "schema_names"));
            t.setOffsets(MinMaxOffsets.from(AvroUtils.getAvroMapOfRecords(data, "offsets")));
            return t;
        }

        private static Map<String, GenericData.Record> create(Map<String, TopicOffsets> offsets) {
            if (offsets == null) {
                return null;
            } else {
                Map<String, GenericData.Record> records = new HashMap<>();
                for (Entry<String, TopicOffsets> e : offsets.entrySet()) {
                    records.put(e.getKey(), e.getValue().create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_topics);
            r.put("topic_name", this.topic_name);
            r.put("schema_names", this.schema_names);
            r.put("offsets", MinMaxOffsets.create(this.offsets));
            return r;
        }

        /**
         * Gets the topic name.
         * @return the topic name
         */
        @JsonProperty("topic_name")
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

        /**
         * Returns a hash code based on the parent function name.
         *
         * @return the hash code for this event set
         */
        @Override
        public int hashCode() { return Objects.hash(this.topic_name); }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof TopicOffsets t) {
                return Objects.equals(this.topic_name, t.topic_name) && 
                    AvroUtils.isEqual(this.schema_names, t.schema_names) && 
                    AvroUtils.isEqual(this.offsets, t.offsets);
            } else {
                return false;
            }
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

        private static Map<String, MinMaxOffsets> from(Map<String, GenericData.Record> data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                Map<String, MinMaxOffsets> m = new HashMap<>();
                for(Entry<String, GenericData.Record> e : data.entrySet()) {
                    m.put(e.getKey(), MinMaxOffsets.from(e.getValue()));
                }
                return m;
            }
        }

        private static MinMaxOffsets from(GenericData.Record data) {
            MinMaxOffsets t = new MinMaxOffsets();
            t.setMin_offset(AvroUtils.getAvroValue(data, "min_offset", Long.class));
            t.setMax_offset(AvroUtils.getAvroValue(data, "max_offset", Long.class));
            t.setPartition(AvroUtils.getAvroValue(data, "partition", Integer.class));
            return t;
        }

        private static Map<String, GenericData.Record> create(Map<String, MinMaxOffsets> offsets) {
            if (offsets == null) {
                return null;
            } else {
                Map<String, GenericData.Record> records = new HashMap<>();
                for (Entry<String, MinMaxOffsets> e : offsets.entrySet()) {
                    records.put(e.getKey(), e.getValue().create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_offset);
            r.put("min_offset", this.min_offset);
            r.put("max_offset", this.max_offset);
            r.put("partition", this.partition);
            return r;
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

        /**
         * Returns a hash code based on the parent function name.
         *
         * @return the hash code for this event set
         */
        @Override
        public int hashCode() { return Objects.hash(this.partition); }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof MinMaxOffsets t) {
                return Objects.equals(this.partition, t.partition) && 
                    Objects.equals(this.min_offset, t.min_offset) && 
                    Objects.equals(this.max_offset, t.max_offset);
            } else {
                return false;
            }
        }
        
    }

}

