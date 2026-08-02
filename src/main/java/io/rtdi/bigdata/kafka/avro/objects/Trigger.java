package io.rtdi.bigdata.kafka.avro.objects;

import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroBoolean;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class Trigger {

    public static ValueSchema trigger_schema = new ValueSchema("trigger", "Trigger for a function to be called when certain events occur");
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
        commit_event.add("delay_seconds", AvroInt.create(), "if a first trigger event occured, wait this many seconds to collect more, thus avoiding frequent triggers", true);
        commit_event.add("idle_seconds", AvroInt.create(), "wait until no trigger event occured for this many seconds and only then start, to avoid frequent triggers", true);

        RecordSchema dataflow = new RecordSchema("dataflow", "the function should be called for all these dataflows");
        dataflow.add("dataflow_name", AvroString.create(), "the dataflow name to pass in as parameter", false);
        dataflow.add("delta_single_trigger", AvroBoolean.create(), "If true, ignore partition list for delta loads, use partitions for initial loads only.", true);
        dataflow.add("partitions", new AvroArray(AvroInt.create()), "the partition parameters to use for this dataflow", true);

        RecordSchema events = new RecordSchema("events", "a function can have different combinations of dataflows and events");
        events.add("on_commit", commit_event, "if provided, run only when a commit for this was found", true);
        events.add("on_dataflow", new AvroArray(AvroString.create()), "if provided, run when a dataflow completed", true);
        events.add("on_schedule", new AvroArray(schedule), "if provided, run only at the specified fixed times (in UTC); schedules and events are OR conditions", true);
        events.add("dataflows", new AvroArray(dataflow), "if provided, call the URL n times, once per dataflow or dataflow/partition", true);

        trigger_schema.add("events", new AvroArray(events), "all combinations of dataflow and trigger events", true);
        trigger_schema.add("function_name", AvroString.create(), "an arbitrary name, often the function or container name", false);
        trigger_schema.add("queuename", AvroString.create(), "the queue name of the function", false);
        trigger_schema.setPrimaryKeys("function_name");
    }

    
    public static class EventSet {
        private List<Schedule> onSchedule;
        private CommitEvent onCommit;
        private List<String> onDataflow;
        private List<Dataflow> dataflows;
        private Triggers _triggers;

        public List<Schedule> getOnSchedule() { return onSchedule; }
        /**
         * Gets the commit event that should trigger the function.
         *
         * @return the commit event configuration, or {@code null} if none is set
         */
        public CommitEvent getOnCommit() { return onCommit; }
        public List<String> getOnDataflow() { return onDataflow; }
        public List<Dataflow> getDataflows() { return dataflows; }
        /**
         * Gets the trigger container that owns this event set.
         *
         * @return the parent trigger definition
         */
        public Triggers getTriggers() { return _triggers; }

        /**
         * Creates an empty event set.
         */
        public EventSet() {
            
        }

        /**
         * Associates this event set with its parent trigger definition.
         *
         * @param t the trigger definition
         */
        public void setTriggers(Triggers t) { this._triggers = t; }

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
         */
        public void addDataflow(String dataflowName, List<Integer> partitions) {
            if (this.dataflows == null) this.dataflows = new ArrayList<>();
            this.dataflows.add(new Dataflow(dataflowName, null, partitions));
        }

        /**
         * Returns a hash code based on the parent function name.
         *
         * @return the hash code for this event set
         */
        @Override
        public int hashCode() { return _triggers != null && _triggers.getFunctionName() != null ? _triggers.getFunctionName().hashCode() : 0; }
    }

    public static class Triggers {
        private String functionName;
        private String queuename;
        private List<EventSet> events = new ArrayList<>();

        /**
         * Creates an empty trigger definition.
         */
        public Triggers() {

        }

        /**
         * Creates a trigger definition for a function name and queue name.
         *
         * @param functionName the function name
         * @param queuename the queue name
         */
        public Triggers(String functionName, String queuename) {
            this.functionName = functionName;
            this.queuename = queuename;
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
        public List<EventSet> getEvents() { return events; }

        /**
         * Creates and registers a new event set for this trigger definition.
         *
         * @return the newly created event set
         */
        public EventSet addEventSet() {
            EventSet e = new EventSet();
            e.setTriggers(this);
            this.events.add(e);
            return e;
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
        public int hashCode() { return functionName != null ? functionName.hashCode() : 0; }
    }

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

        public List<String> getWeekdays() { return weekdays; }
        /**
         * Sets the weekdays included in the schedule.
         *
         * @param weekdays the weekday names
         */
        public void setWeekdays(List<String> weekdays) { this.weekdays = weekdays; }
        public List<Integer> getHours() { return hours; }
        /**
         * Sets the hours included in the schedule.
         *
         * @param hours the hour values
         */
        public void setHours(List<Integer> hours) { this.hours = hours; }
        public List<Integer> getMinutes() { return minutes; }
        /**
         * Sets the minutes included in the schedule.
         *
         * @param minutes the minute values
         */
        public void setMinutes(List<Integer> minutes) { this.minutes = minutes; }
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
        public Boolean getLastDayOfMonth() { return lastDayOfMonth; }
        /**
         * Sets whether the schedule should include the last day of the month.
         *
         * @param lastDayOfMonth the last-day-of-month flag
         */
        public void setLastDayOfMonth(Boolean lastDayOfMonth) { this.lastDayOfMonth = lastDayOfMonth; }
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
    }

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

        public List<String> getSchemaNames() { return schemaNames; }
        public List<String> getTopicPartitions() { return topicPartitions; }
        /**
         * Gets the delay in seconds before collecting additional trigger events.
         *
         * @return the delay in seconds
         */
        public Integer getDelaySeconds() { return delaySeconds; }
        /**
         * Gets the idle time in seconds before starting the trigger.
         *
         * @return the idle time in seconds
         */
        public Integer getIdleSeconds() { return idleSeconds; }

        /**
         * Returns a readable description of the commit event.
         *
         * @return the commit event description
         */
        @Override
        public String toString() { return "CommitEvent: " + schemaNames; }
    }

    public static class Dataflow {
        private String dataflowName;
        private Boolean deltaSingleTrigger;
        private List<Integer> partitions;

        /**
         * Creates an empty dataflow trigger configuration.
         */
        public Dataflow() {

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
        public Boolean getDeltaSingleTrigger() { return deltaSingleTrigger; }
        /**
         * Sets whether delta loads should ignore the partition list and only use partitions for initial loads.
         *
         * @param deltaSingleTrigger the delta-single-trigger flag
         */
        public void setDeltaSingleTrigger(Boolean deltaSingleTrigger) { this.deltaSingleTrigger = deltaSingleTrigger; }
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
    }
}
