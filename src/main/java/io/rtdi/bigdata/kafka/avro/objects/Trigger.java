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
         * Executes the CommitEvent getOnCommit operation.
         */
        public CommitEvent getOnCommit() { return onCommit; }
        public List<String> getOnDataflow() { return onDataflow; }
        public List<Dataflow> getDataflows() { return dataflows; }
        /**
         * Executes the Triggers getTriggers operation.
         */
        public Triggers getTriggers() { return _triggers; }

        /**
         * Executes the EventSet operation.
         */
        public EventSet() {
            
        }

        /**
         * Executes the void setTriggers operation.
         * @param t the parameter value
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
         * Executes the void addDataflowEvent operation.
         * @param dataflowName the parameter value
         */
        public void addDataflowEvent(String dataflowName) {
            if (this.onDataflow == null) this.onDataflow = new ArrayList<>();
            this.onDataflow.add(dataflowName);
        }

        /**
         * Executes the void setCommitEvent operation.
         * @param schemaNames the parameter value
         * @param topicPartitions the parameter value
         * @param delaySeconds the parameter value
         * @param idleSeconds the parameter value
         */
        public void setCommitEvent(List<String> schemaNames, List<String> topicPartitions, Integer delaySeconds, Integer idleSeconds) {
            this.onCommit = new CommitEvent(schemaNames, topicPartitions, delaySeconds, idleSeconds);
        }

        /**
         * Executes the void addDataflow operation.
         * @param dataflowName the parameter value
         * @param partitions the parameter value
         */
        public void addDataflow(String dataflowName, List<Integer> partitions) {
            if (this.dataflows == null) this.dataflows = new ArrayList<>();
            this.dataflows.add(new Dataflow(dataflowName, null, partitions));
        }

        @Override
        /**
         * Executes the int hashCode operation.
         */
        public int hashCode() { return _triggers != null && _triggers.getFunctionName() != null ? _triggers.getFunctionName().hashCode() : 0; }
    }

    public static class Triggers {
        private String functionName;
        private String queuename;
        private List<EventSet> events = new ArrayList<>();

        /**
         * Executes the Triggers operation.
         */
        public Triggers() {

        }

        /**
         * Executes the Triggers operation.
         * @param functionName the parameter value
         * @param queuename the parameter value
         */
        public Triggers(String functionName, String queuename) {
            this.functionName = functionName;
            this.queuename = queuename;
        }

        /**
         * Executes the String getFunctionName operation.
         */
        public String getFunctionName() { return functionName; }
        /**
         * Executes the void setFunctionName operation.
         * @param functionName the parameter value
         */
        public void setFunctionName(String functionName) { this.functionName = functionName; }
        /**
         * Executes the String getQueuename operation.
         */
        public String getQueuename() { return queuename; }
        /**
         * Executes the void setQueuename operation.
         * @param queuename the parameter value
         */
        public void setQueuename(String queuename) { this.queuename = queuename; }
        public List<EventSet> getEvents() { return events; }

        /**
         * Executes the EventSet addEventSet operation.
         */
        public EventSet addEventSet() {
            EventSet e = new EventSet();
            e.setTriggers(this);
            this.events.add(e);
            return e;
        }

        @Override
        /**
         * Executes the String toString operation.
         */
        public String toString() { return "Triggers for function " + functionName; }

        @Override
        /**
         * Executes the int hashCode operation.
         */
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
         * Executes the Schedule operation.
         */
        public Schedule() {
            
        }

        public List<String> getWeekdays() { return weekdays; }
        /**
         * Executes the void setWeekdays operation.
         * @param weekdays the parameter value
         */
        public void setWeekdays(List<String> weekdays) { this.weekdays = weekdays; }
        public List<Integer> getHours() { return hours; }
        /**
         * Executes the void setHours operation.
         * @param hours the parameter value
         */
        public void setHours(List<Integer> hours) { this.hours = hours; }
        public List<Integer> getMinutes() { return minutes; }
        /**
         * Executes the void setMinutes operation.
         * @param minutes the parameter value
         */
        public void setMinutes(List<Integer> minutes) { this.minutes = minutes; }
        public List<Integer> getDays() { return days; }
        /**
         * Executes the void setDays operation.
         * @param days the parameter value
         */
        public void setDays(List<Integer> days) { this.days = days; }
        /**
         * Executes the Boolean getLastDayOfMonth operation.
         */
        public Boolean getLastDayOfMonth() { return lastDayOfMonth; }
        /**
         * Executes the void setLastDayOfMonth operation.
         * @param lastDayOfMonth the parameter value
         */
        public void setLastDayOfMonth(Boolean lastDayOfMonth) { this.lastDayOfMonth = lastDayOfMonth; }
        public List<Integer> getMonths() { return months; }
        /**
         * Executes the void setMonths operation.
         * @param months the parameter value
         */
        public void setMonths(List<Integer> months) { this.months = months; }



        @Override
        /**
         * Executes the String toString operation.
         */
        public String toString() { return "Schedule"; }
    }

    public static class CommitEvent {
        private List<String> schemaNames;
        private List<String> topicPartitions;
        private Integer delaySeconds;
        private Integer idleSeconds;

        /**
         * Executes the CommitEvent operation.
         */
        public CommitEvent() {

        }

        /**
         * Executes the CommitEvent operation.
         * @param schemaNames the parameter value
         * @param topicPartitions the parameter value
         * @param delaySeconds the parameter value
         * @param idleSeconds the parameter value
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
         * Executes the Integer getDelaySeconds operation.
         */
        public Integer getDelaySeconds() { return delaySeconds; }
        /**
         * Executes the Integer getIdleSeconds operation.
         */
        public Integer getIdleSeconds() { return idleSeconds; }

        @Override
        /**
         * Executes the String toString operation.
         */
        public String toString() { return "CommitEvent: " + schemaNames; }
    }

    public static class Dataflow {
        private String dataflowName;
        private Boolean deltaSingleTrigger;
        private List<Integer> partitions;

        /**
         * Executes the Dataflow operation.
         */
        public Dataflow() {

        }

        /**
         * Executes the Dataflow operation.
         * @param dataflowName the parameter value
         * @param deltaSingleTrigger the parameter value
         * @param partitions the parameter value
         */
        public Dataflow(String dataflowName, Boolean deltaSingleTrigger, List<Integer> partitions) {
            this.dataflowName = dataflowName;
            this.deltaSingleTrigger = deltaSingleTrigger;
            this.partitions = partitions;
        }

        /**
         * Executes the String getDataflowName operation.
         */
        public String getDataflowName() { return dataflowName; }
        /**
         * Executes the void setDataflowName operation.
         * @param dataflowName the parameter value
         */
        public void setDataflowName(String dataflowName) { this.dataflowName = dataflowName; }
        /**
         * Executes the Boolean getDeltaSingleTrigger operation.
         */
        public Boolean getDeltaSingleTrigger() { return deltaSingleTrigger; }
        /**
         * Executes the void setDeltaSingleTrigger operation.
         * @param deltaSingleTrigger the parameter value
         */
        public void setDeltaSingleTrigger(Boolean deltaSingleTrigger) { this.deltaSingleTrigger = deltaSingleTrigger; }
        public List<Integer> getPartitions() { return partitions; }
        /**
         * Executes the void setPartitions operation.
         * @param partitions the parameter value
         */
        public void setPartitions(List<Integer> partitions) { this.partitions = partitions; }

        @Override
        /**
         * Executes the String toString operation.
         */
        public String toString() { return "Dataflow: " + dataflowName; }
    }
}
