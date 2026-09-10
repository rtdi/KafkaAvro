package io.rtdi.bigdata.kafka.avro.objects;

import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroBoolean;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * Stores the information when to call a function.
 */
public class Trigger {
    private String functionName;
    private String queuename;
    private List<EventSet> events = new ArrayList<>();
    /**
     * The Avro schema for the trigger definition.
     */
    public static final ValueSchema trigger_schema = new ValueSchema("trigger", "Trigger for a function to be called when certain events occur");
    public static final Schema avro_schema;
    private static final Schema avro_schema_eventset;
    private static final Schema avro_schema_commit_event;
    private static final Schema avro_schema_schedule;
    private static final Schema avro_schema_dataflow;
    static {
        RecordSchema schedule = new RecordSchema("schedule", "all conditions within the fields must be met to run, they are AND conditions");
        schedule.add("weekdays", new AvroArray(AvroString.create()), "if provided, run only on these days, e.g. Mon-Fri", true);
        schedule.add("hours", new AvroArray(AvroInt.create()), "if provided, run only on these hours of the day", true);
        schedule.add("minutes", new AvroArray(AvroInt.create()), "if provided, run only on these minutes of the hours", true);
        schedule.add("days", new AvroArray(AvroInt.create()), "if provided, run only on these days of the month", true);
        schedule.add("last_day_of_month", AvroBoolean.create(), "an additional day to the days array", true);
        schedule.add("months", new AvroArray(AvroInt.create()), "if provided, run only on these months", true);

        RecordSchema commit_event = new RecordSchema("on_commit", "trigger if a commit of that type was issued");
        commit_event.add("schema_names", new AvroArray(AvroString.create()), "trigger when the table with this schema name has a commit", true);
        commit_event.add("topic_partitions", new AvroArray(AvroString.create()), "only trigger if data was in any of these topic/partitions", true);

        RecordSchema dataflow = new RecordSchema("dataflow", "the function should be called for all these dataflows");
        dataflow.add("dataflow_name", AvroString.create(), "the dataflow name to pass in as parameter", false);
        dataflow.add("delta_single_trigger", AvroBoolean.create(), "If true, ignore partition list for delta loads, use partitions for initial loads only.", true);
        dataflow.add("partitions", new AvroArray(AvroInt.create()), "the partition parameters to use for this dataflow", true);

        RecordSchema ondataflow = new RecordSchema("dataflow", "the function should be called for all these dataflows");
        ondataflow.add("function_name", AvroString.create(), "the function name triggering this event", false);
        ondataflow.add("dataflow_name", AvroString.create(), "the dataflow triggering this event", true);


        RecordSchema events = new RecordSchema("events", "a function can have different combinations of dataflows and events");
        events.add("on_commit", commit_event, "if provided, run only when a commit for this was found", true);
        events.add("on_dataflows", new AvroArray(ondataflow), "if provided, run when these dataflows completed", true);
        events.add("on_schedule", new AvroArray(schedule), "if provided, run only at the specified fixed times (in UTC); schedules and events are OR conditions", true);
        events.add("dataflows", new AvroArray(dataflow), "if provided, call the URL n times, once per dataflow or dataflow/partition", true);
        events.add("delay_seconds", AvroInt.create(), "if a first trigger event occured, wait this many seconds to collect more, thus avoiding frequent triggers", true);
        events.add("idle_seconds", AvroInt.create(), "wait until no trigger event occured for this many seconds and only then start, to avoid frequent triggers", true);

        trigger_schema.add("events", new AvroArray(events), "all combinations of dataflow and trigger events", true);
        trigger_schema.add("function_name", AvroString.create(), "an arbitrary name, often the function or container name", false);
        trigger_schema.add("endpoint", AvroString.create(), "the queue name of the function", false)
            .aliases("queuename");
        trigger_schema.setPrimaryKeys("function_name");
        avro_schema = trigger_schema.createSchema();
        avro_schema_eventset = AvroUtils.getBaseSchema(avro_schema.getField("events").schema()).getElementType();
        avro_schema_commit_event = AvroUtils.getBaseSchema(avro_schema_eventset.getField("on_commit").schema());
        avro_schema_schedule = AvroUtils.getBaseSchema(avro_schema_eventset.getField("on_schedule").schema()).getElementType();
        avro_schema_dataflow = AvroUtils.getBaseSchema(avro_schema_eventset.getField("dataflows").schema()).getElementType();
    }

    /**
     * Constructor for triggers
     */
    public Trigger() {
    }

    /**
     * Creates a trigger definition for a function name and queue name.
     *
     * @param functionName the function name
     * @param queuename the queue name
     */
    public Trigger(String functionName, String queuename) {
        this.functionName = functionName;
        this.queuename = queuename;
    }

    /**
     * Get the current object as GenericRecord
     * @return GenericRecord
     */
    public GenericData.Record toRecord() {
        GenericData.Record r = new GenericData.Record(avro_schema);
        r.put("events", EventSet.create(events));
        r.put("function_name", this.functionName);
        r.put("queuename", this.queuename);
        return r;
    }

    /**
     * Create a trigger instance from the Avro record data
     * 
     * @param data Avro record
     * @return the Trigger instance
     * @throws AvroTypeException in case the record does not match the structure
     */
    public static Trigger from(GenericData.Record data) throws AvroTypeException {
        Trigger t = new Trigger();
        t.setFunctionName(AvroUtils.getAvroValue(data, "function_name", String.class));
        t.setQueuename(AvroUtils.getAvroValue(data, "queuename", String.class));
        t.setEvents(EventSet.from(AvroUtils.getAvroListOfRecords(data, "events")));
        return t;
    }

    /**
     * Gets the function name for this trigger definition.
     *
     * @return the function name
     */
    public String getFunctionName() { return functionName; }

    /**
     * Sets the function name for this trigger definition.
     *
     * @param functionName the function name
     */
    public void setFunctionName(String functionName) { this.functionName = functionName; }

    /**
     * Gets the queue name associated with this trigger definition.
     *
     * @return the queue name
     */
    public String getQueuename() { return queuename; }

    /**
     * Sets the queue name associated with this trigger definition.
     *
     * @param queuename the queue name
     */
    public void setQueuename(String queuename) { this.queuename = queuename; }

    /**
     * get all event sets
     * @return all event sets
     */
    public List<EventSet> getEvents() { return events; }

    /**
     * Creates and registers a new event set for this trigger definition.
     *
     * @return the newly created event set
     */
    public EventSet addEventSet() {
        EventSet e = new EventSet();
        this.events.add(e);
        return e;
    }

    /**
     * Add this list as event set and update the parent trigger for each
     * @param eventsets list of all eventsets
     */
    public void setEvents(List<EventSet> eventsets) {
        this.events = eventsets;
    }

    /**
     * Returns a readable description of the trigger definition.
     *
     * @return the trigger description
     */
    @Override
    public String toString() { return "Triggers for function " + functionName; }

    /**
     * Returns a hash code based on the function name.
     *
     * @return the hash code for this trigger definition
     */
    @Override
    public int hashCode() { return Objects.hashCode(functionName); }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof Trigger t) {
            return Objects.equals(this.functionName, t.functionName) && Objects.equals(this.queuename, t.queuename) &&
                AvroUtils.isEqual(this.events, t.events);
        } else {
            return false;
        }
    }

    	/**
	 * Formats the current schema as pretty-printed Avro JSON.
	 *
	 * @return the schema formatted as Avro JSON
	 */
	public String toAvroJson() {
		return Trigger.trigger_schema.toAvroJson();
	}

    /**
     * Get the constant ValueSchema for the Trigger
     * @return the constant value schema
     */
    @JsonIgnore
    public ValueSchema getValueSchema() {
        return Trigger.trigger_schema;
    }

    /**
     * Get the constant Avro Schema for this record
     * @return the Avro schema
     */
    @JsonIgnore
    public Schema getSchema() {
        return Trigger.avro_schema;
    }

	/**
	 * Deserializes a JSON payload into a {@link Trigger} instance.
	 *
	 * @param json the JSON payload
	 * @return the parsed value schema
	 * @throws JsonMappingException if the JSON structure cannot be mapped
	 * @throws JsonProcessingException if the JSON payload cannot be read
	 */
	public static Trigger fromRecordJson(String json) throws JsonMappingException, JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.readValue(json, Trigger.class);
	}

	/**
	 * Create the trigger instance from an Avro record.
	 *
	 * @param data the Avro record witht the data
	 * @return the Trigger instance
	 */
	public static Trigger fromRecord(GenericData.Record data) {
        Trigger t = new Trigger();
        t.setFunctionName(AvroUtils.getAvroValue(data, "function_name", String.class));
        t.setQueuename(AvroUtils.getAvroValue(data, "queue_name", String.class));
        t.setEvents(EventSet.from(AvroUtils.castListOfRecords(t)));
        return t;
	}


    /**
     * Serialize the record values into Json
     * @return the Trigger values as string
     * @throws JsonProcessingException
     */
    public String toRecordJson() throws JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.writeValueAsString(this);
    }

    /**
     * A trigger consists of 0..n event sets, which are different reasons to call the function.
     */
    public static class EventSet {
        private List<Schedule> onSchedule;
        private CommitEvent onCommit;
        private List<String> onDataflow;
        private List<Dataflow> dataflows;

        private static List<GenericData.Record> create(List<EventSet> events) {
            if (events == null) {
                return null;
            } else {
                List<GenericData.Record> records = new ArrayList<>();
                for (EventSet e : events) {
                    records.add(e.create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_eventset);
            r.put("on_commit", CommitEvent.create(this.onCommit));
            r.put("on_schedule", Schedule.create(this.onSchedule));
            r.put("on_dataflow", this.onDataflow);
            r.put("dataflows", Dataflow.create(this.dataflows));
            return r;
        }

        /**
         * Create the EventSet instance from an Avro record.
         *
         * @param data the Avro record witht the data
         * @return the EventSet instance
         * @throws AvroTypeException if the generic record and the type does not match
         */
        private static EventSet from(GenericData.Record data) throws AvroTypeException {
            EventSet t = new EventSet();
            t.setOnCommit(CommitEvent.from(AvroUtils.getAvroRecord(data, "on_commit")));
            t.setOnDataflow(AvroUtils.getAvroListOfString(data, "on_dataflow"));
            t.setOnSchedule(Schedule.from(AvroUtils.getAvroListOfRecords(data, "on_schedule")));
            t.setDataflows(Dataflow.from(AvroUtils.getAvroListOfRecords(data, "dataflows")));
            return t;
        }

        private static List<EventSet> from(List<?> data) {
            List<EventSet> l = new ArrayList<>();
            for (Object o : data) {
                if (o instanceof GenericData.Record r) {
                    l.add(EventSet.from(r));
                }
            }
            return l;
        }

        /**
         * Gets the list of schedules that should trigger the function.
         * @return the list of schedules, or {@code null} if none are set
         */
        @JsonProperty("on_schedule")
        public List<Schedule> getOnSchedule() { return onSchedule; }

        /**
         * Gets the commit event that should trigger the function.
         * @return the commit event configuration, or {@code null} if none is set
         */
        @JsonProperty("on_commit")
        public CommitEvent getOnCommit() { return onCommit; }

        /**
         * Gets the list of dataflow names that should trigger the function.
         * @return the list of dataflow names, or {@code null} if none are set
         */
        @JsonProperty("on_dataflow")
        public List<String> getOnDataflow() { return onDataflow; }

        /**
         * Gets the list of dataflow trigger configurations.
         * @return the list of dataflow configurations, or {@code null} if none are set
         */
        public List<Dataflow> getDataflows() { return dataflows; }
        
        /**
         * Sets the list of schedules that should trigger the function.
         * @param onSchedule the list of schedules, or {@code null} if none are set
         */
        public void setOnSchedule(List<Schedule> onSchedule) {
            this.onSchedule = onSchedule;
        }

        /**
         * Sets the commit event that should trigger the function.
         * @param onCommit the commit event configuration, or {@code null} if none is set
         */
        public void setOnCommit(CommitEvent onCommit) {
            this.onCommit = onCommit;
        }

        /**
         * Sets the list of dataflow names that should trigger the function.
         * @param onDataflow the list of dataflow names, or {@code null} if none are set
         */
        public void setOnDataflow(List<String> onDataflow) {
            this.onDataflow = onDataflow;
        }

        /**
         * Sets the list of dataflow trigger configurations.
         * @param dataflows the list of dataflow configurations, or {@code null} if none are set
         */
        public void setDataflows(List<Dataflow> dataflows) {
            this.dataflows = dataflows;
        }

        /**
         * Creates an empty event set.
         */
        public EventSet() {
            
        }

        /**
         * Adds a schedule to the event set with the specified parameters.
         * @param weekdays on which weekdays the dataflow should run
         * @param hours the hours to run
         * @param minutes the minutes to run
         * @param days the calendar days to run
         * @param months the months to run
         * @param lastDayOfMonth true if it should run on the last day of a month
         */
        public void addSchedule(List<DayOfWeek> weekdays, List<Integer> hours, List<Integer> minutes,
                                List<Integer> days, List<Integer> months, Boolean lastDayOfMonth) {
            Schedule s = new Schedule();
            if (weekdays != null) {
                s.setWeekdays(new ArrayList<>());
                for (DayOfWeek w : weekdays) s.getWeekdays().add(w.name());
            }
            s.setHours(hours);
            s.setMinutes(minutes);
            s.setDays(days);
            s.setLastDayOfMonth(lastDayOfMonth);
            s.setMonths(months);
            if (this.onSchedule == null) this.onSchedule = new ArrayList<>();
            this.onSchedule.add(s);
        }

        /**
         * Adds a dataflow name that should trigger the function when that dataflow completes.
         *
         * @param dataflowName the dataflow name
         */
        public void addDataflowEvent(String dataflowName) {
            if (this.onDataflow == null) this.onDataflow = new ArrayList<>();
            this.onDataflow.add(dataflowName);
        }

        /**
         * Creates a commit event definition with schema names, topic partitions, and delay settings.
         *
         * @param delaySeconds the delay before evaluating a first trigger for a burst of events
         * @param idleSeconds the idle time before starting the trigger after a pause
         * @param schemaNames the schema names to watch for commits
          */
        public void addCommitEvent(Integer delaySeconds, Integer idleSeconds, String... schemaNames) {
            this.onCommit = new CommitEvent(Arrays.asList(schemaNames), null, delaySeconds, idleSeconds);
        }

        /**
         * Sets the commit-based trigger event.
         *
         * @param schemaNames the schema names that should trigger the event
         * @param topicPartitions the topic partitions to monitor
         * @param delaySeconds the number of seconds to wait before evaluating a trigger event
         * @param idleSeconds the number of idle seconds before starting the trigger
         */
        public void setCommitEvent(List<String> schemaNames, List<String> topicPartitions, Integer delaySeconds, Integer idleSeconds) {
            this.onCommit = new CommitEvent(schemaNames, topicPartitions, delaySeconds, idleSeconds);
        }

        /**
         * Adds a dataflow trigger configuration for a given dataflow and partition list.
         *
         * @param dataflowName the dataflow name
         * @param partitions the partition numbers to use with the dataflow
         * @param delta_single_trigger true, if only the initial load should be triggered once per partition
         */
        public void addDataflow(String dataflowName, Boolean delta_single_trigger, List<Integer> partitions) {
            if (this.dataflows == null) this.dataflows = new ArrayList<>();
            this.dataflows.add(new Dataflow(dataflowName, delta_single_trigger, partitions));
        }

        /**
         * Returns a hash code based on the parent function name.
         *
         * @return the hash code for this event set
         */
        @Override
        public int hashCode() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof EventSet t) {
                return Objects.equals(this.onCommit, t.onCommit) && 
                    AvroUtils.isEqual(this.onSchedule, t.onSchedule) && 
                    AvroUtils.isEqual(this.onDataflow, t.onDataflow) && 
                    AvroUtils.isEqual(this.dataflows, t.dataflows);
            } else {
                return false;
            }
        }
    }


    /**
     * Schedule based triggers
     */
    public static class Schedule {
        private List<String> weekdays;
        private List<Integer> hours;
        private List<Integer> minutes;
        private List<Integer> days;
        private Boolean lastDayOfMonth;
        private List<Integer> months;

        /**
         * Creates an empty schedule.
         */
        public Schedule() {
        }

        private static List<Schedule> from(List<GenericData.Record> data) {
            if (data == null) {
                return null;
            } else {
                List<Schedule> l = new ArrayList<>();
                for (GenericData.Record o : data) {
                    l.add(Schedule.from(o));
                }
                return l;
            }
        }

        /**
         * Create the Schedule instance from an Avro record.
         *
         * @param data the Avro record witht the data
         * @return the Trigger instance
         */
        private static Schedule from(GenericData.Record data) {
            Schedule t = new Schedule();
            t.setWeekdays(AvroUtils.castListType(data.get("weekdays"), String.class));
            t.setHours(AvroUtils.castListType(data.get("hours"), Integer.class));
            t.setMinutes(AvroUtils.castListType(data.get("minutes"), Integer.class));
            t.setDays(AvroUtils.castListType(data.get("days"), Integer.class));
            t.setMonths(AvroUtils.castListType(data.get("months"), Integer.class));
            t.setLastDayOfMonth(AvroUtils.castType(data.get("last_day_of_month"), Boolean.class));
            return t;
        }

        private static List<GenericData.Record> create(List<Schedule> onSchedule) {
            if (onSchedule == null) {
                return null;
            } else {
                List<GenericData.Record> records = new ArrayList<>();
                for (Schedule e : onSchedule) {
                    records.add(e.create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_schedule);
            r.put("weekdays", this.weekdays);
            r.put("hours", this.hours);
            r.put("minutes", this.minutes);
            r.put("days", this.days);
            r.put("last_day_of_month", this.lastDayOfMonth);
            r.put("months", this.months);
            return r;
         }


        /**
         * get all weeksdays for triger
         * @return list of weekdays
         */
        public List<String> getWeekdays() { return weekdays; }

        /**
         * Sets the weekdays included in the schedule.
         *
         * @param weekdays the weekday names
         */
        public void setWeekdays(List<String> weekdays) { this.weekdays = weekdays; }

        /**
         * get all hours of the schedule
         * @return list of hours
         */
        public List<Integer> getHours() { return hours; }

        /**
         * Sets the hours included in the schedule.
         *
         * @param hours the hour values
         */
        public void setHours(List<Integer> hours) { this.hours = hours; }

        /**
         * get minutes of the schedule
         * @return list of minutes
         */
        public List<Integer> getMinutes() { return minutes; }

        /**
         * Sets the minutes included in the schedule.
         *
         * @param minutes the minute values
         */
        public void setMinutes(List<Integer> minutes) { this.minutes = minutes; }

        /**
         * get lsit of days of the schedule
         * @return list of calender days
         */
        public List<Integer> getDays() { return days; }

        /**
         * Sets the day-of-month values included in the schedule.
         *
         * @param days the day values
         */
        public void setDays(List<Integer> days) { this.days = days; }
        /**
         * Gets whether the schedule should include the last day of the month.
         *
         * @return the last-day-of-month flag
         */
        @JsonProperty("last_day_of_month")
        public Boolean getLastDayOfMonth() { return lastDayOfMonth; }

        /**
         * Sets whether the schedule should include the last day of the month.
         *
         * @param lastDayOfMonth the last-day-of-month flag
         */
        public void setLastDayOfMonth(Boolean lastDayOfMonth) { this.lastDayOfMonth = lastDayOfMonth; }

        /**
         * get the list of months of the schedule
         * @return list of months
         */
        public List<Integer> getMonths() { return months; }

        /**
         * Sets the months included in the schedule.
         *
         * @param months the month values
         */
        public void setMonths(List<Integer> months) { this.months = months; }

        /**
         * Returns a readable description of the schedule.
         *
         * @return the schedule description
         */
        @Override
        public String toString() { return "Schedule"; }

        @Override
        public int hashCode() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Schedule t) {
                return Objects.equals(this.lastDayOfMonth, t.lastDayOfMonth) && 
                    AvroUtils.isEqual(this.weekdays, t.weekdays) && 
                    AvroUtils.isEqual(this.hours, t.hours) && 
                    AvroUtils.isEqual(this.minutes, t.minutes) && 
                    AvroUtils.isEqual(this.days, t.days) && 
                    AvroUtils.isEqual(this.months, t.months);
            } else {
                return false;
            }
        }

    }

    /**
     * Commit event
     */
    public static class CommitEvent {
        private List<String> schemaNames;
        private List<String> topicPartitions;
        private Integer delaySeconds;
        private Integer idleSeconds;

        /**
         * Creates an empty commit event.
         */
        public CommitEvent() {
        }

        /**
         * Create a CommitEvent instance from the Avro record
         * @param data avro record
         * @return new CommitEvent instance
         */
        public static CommitEvent from(GenericData.Record data) {
            CommitEvent c = new CommitEvent();
            c.setSchemaNames(AvroUtils.castListType(data.get("schema_names"), String.class));
            c.setTopicPartitions(AvroUtils.castListType(data.get("topic_partitions"), String.class));
            c.setDelaySeconds(AvroUtils.getAvroValue(data, "delay_seconds", Integer.class));
            c.setIdleSeconds(AvroUtils.getAvroValue(data, "idle_seconds", Integer.class));
            return c;
        }

        private static GenericData.Record create(CommitEvent onCommit) {
            if (onCommit == null) {
                return null;
            } else {
                return onCommit.create();
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_commit_event);
            r.put("schema_names", this.schemaNames);
            r.put("topic_partitions", this.topicPartitions);
            r.put("delay_seconds", this.delaySeconds);
            r.put("idle_seconds", this.idleSeconds);
            return r;
        }

        /**
         * Creates a commit event definition with schema names, topic partitions, and delay settings.
         *
         * @param schemaNames the schema names to watch for commits
         * @param topicPartitions the topic partitions to watch for commits
         * @param delaySeconds the delay before evaluating a first trigger for a burst of events
         * @param idleSeconds the idle time before starting the trigger after a pause
         */
        public CommitEvent(List<String> schemaNames, List<String> topicPartitions, Integer delaySeconds, Integer idleSeconds) {
            this.schemaNames = schemaNames;
            this.topicPartitions = topicPartitions;
            this.delaySeconds = delaySeconds;
            this.idleSeconds = idleSeconds;
        }

        /**
         * Creates a commit event definition with schema names, topic partitions, and delay settings.
         *
         * @param schemaName the schema to watch for commits
         * @param delaySeconds the delay before evaluating a first trigger for a burst of events
         * @param idleSeconds the idle time before starting the trigger after a pause
         */
        public CommitEvent(String schemaName, Integer delaySeconds, Integer idleSeconds) {
            this(Arrays.asList(schemaName), null, delaySeconds, idleSeconds);
        }

        /**
         * get the list of loaded schemas
         * @return list of schemas
         */
        @JsonProperty("schema_names")
        public List<String> getSchemaNames() { return schemaNames; }

        /**
         * get the topic partitions loaded
         * @return list of topics
         */
        @JsonProperty("topic_partitions")
        public List<String> getTopicPartitions() { return topicPartitions; }

        /**
         * Gets the delay in seconds before collecting additional trigger events.
         *
         * @return the delay in seconds
         */
        @JsonProperty("delay_seconds")
        public Integer getDelaySeconds() { return delaySeconds; }
        
        /**
         * Gets the idle time in seconds before starting the trigger.
         *
         * @return the idle time in seconds
         */
        @JsonProperty("idle_seconds")
        public Integer getIdleSeconds() { return idleSeconds; }

        /**
         * set schema names
         * @param schemaNames list of schema names
         */
        public void setSchemaNames(List<String> schemaNames) {
            this.schemaNames = schemaNames;
        }

        /**
         * set the list of topic partitions
         * @param topicPartitions list of topic partitions
         */
        public void setTopicPartitions(List<String> topicPartitions) {
            this.topicPartitions = topicPartitions;
        }

        /**
         * set the delay of seconds
         * @param delaySeconds wait this many seconds
         */
        public void setDelaySeconds(Integer delaySeconds) {
            this.delaySeconds = delaySeconds;
        }

        /**
         * set the delay of seconds for which multiple commits are collected
         * @param idleSeconds collect for n seconds
         */
        public void setIdleSeconds(Integer idleSeconds) {
            this.idleSeconds = idleSeconds;
        }

        /**
         * Returns a readable description of the commit event.
         *
         * @return the commit event description
         */
        @Override
        public String toString() { return "CommitEvent: " + schemaNames; }

        @Override
        public int hashCode() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof CommitEvent t) {
                return Objects.equals(this.delaySeconds, t.delaySeconds) &&
                    Objects.equals(this.idleSeconds, t.idleSeconds) && 
                    AvroUtils.isEqual(this.schemaNames, t.schemaNames) && 
                    AvroUtils.isEqual(this.topicPartitions, t.topicPartitions);
            } else {
                return false;
            }
        }
    }

    /**
     * Dataflow event
     */
    public static class Dataflow {
        private String dataflowName;
        private Boolean deltaSingleTrigger;
        private List<Integer> partitions;

        /**
         * Creates an empty dataflow trigger configuration.
         */
        public Dataflow() {
        }

        private static List<Dataflow> from(List<GenericData.Record> data) {
            if (data == null) {
                return null;
            } else {
                List<Dataflow> l = new ArrayList<>();
                for (GenericData.Record o : data) {
                    l.add(Dataflow.from(o));
                }
                return l;
            }
        }

        private static Dataflow from(GenericData.Record data) {
            Dataflow d = new Dataflow();
            d.setDataflowName(AvroUtils.getAvroValue(data, "dataflow_name", String.class));
            d.setDeltaSingleTrigger(AvroUtils.getAvroValue(data, "delta_single_trigger", Boolean.class));
            d.setPartitions(AvroUtils.getAvroListOfInteger(data, "partitions"));
            return d;
        }

        private static List<GenericData.Record> create(List<Dataflow> dataflows) {
            if (dataflows == null) {
                return null;
            } else {
                List<GenericData.Record> records = new ArrayList<>();
                for (Dataflow e : dataflows) {
                    records.add(e.create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_dataflow);
            r.put("dataflow_name", this.dataflowName);
            r.put("delta_single_trigger", this.deltaSingleTrigger);
            r.put("partitions", this.partitions);
            return r;
         }

        /**
         * Creates a dataflow trigger configuration.
         *
         * @param dataflowName the dataflow name
         * @param deltaSingleTrigger whether delta loads should ignore the partition list
         * @param partitions the partition values to use for the dataflow
         */
        public Dataflow(String dataflowName, Boolean deltaSingleTrigger, List<Integer> partitions) {
            this.dataflowName = dataflowName;
            this.deltaSingleTrigger = deltaSingleTrigger;
            this.partitions = partitions;
        }

        /**
         * Gets the dataflow name passed to the function.
         *
         * @return the dataflow name
         */
        @JsonProperty("dataflow_name")
        public String getDataflowName() { return dataflowName; }

        /**
         * Sets the dataflow name passed to the function.
         *
         * @param dataflowName the dataflow name
         */
        public void setDataflowName(String dataflowName) { this.dataflowName = dataflowName; }

        /**
         * Gets whether delta loads should ignore the partition list and only use partitions for initial loads.
         *
         * @return the delta-single-trigger flag
         */
        @JsonProperty("delta_single_trigger")
        public Boolean getDeltaSingleTrigger() { return deltaSingleTrigger; }

        /**
         * Sets whether delta loads should ignore the partition list and only use partitions for initial loads.
         *
         * @param deltaSingleTrigger the delta-single-trigger flag
         */
        public void setDeltaSingleTrigger(Boolean deltaSingleTrigger) { this.deltaSingleTrigger = deltaSingleTrigger; }

        /**
         * Get the list of partitions to generate trigger messages for
         * @return list of partitions
         */
        public List<Integer> getPartitions() { return partitions; }
        /**
         * Sets the partition list used for the dataflow trigger.
         *
         * @param partitions the partition numbers
         */
        public void setPartitions(List<Integer> partitions) { this.partitions = partitions; }

        /**
         * Returns a readable description of the dataflow trigger.
         *
         * @return the dataflow description
         */
        @Override
        public String toString() { return "Dataflow: " + dataflowName; }

        @Override
        public int hashCode() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Dataflow t) {
                return Objects.equals(this.dataflowName, t.dataflowName) &&
                    Objects.equals(this.deltaSingleTrigger, t.deltaSingleTrigger) && 
                    AvroUtils.isEqual(this.partitions, t.partitions);
            } else {
                return false;
            }
        }
    }
}
