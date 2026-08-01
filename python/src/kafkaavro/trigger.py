from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Self

from kafkaavro.schemabuilder import ValueSchema, ROW_TYPE_FIELD
from kafkaavro import avro_datatypes
from pydantic import BaseModel, model_validator


class Weekdays(Enum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6

class ExecutionLogState(Enum):
    STARTED = 0
    COMPLETED = 1
    RETRY = 2
    FAILED = 3

class CalledReason(Enum):
    MANUAL = "MANUAL"
    DATAFLOW = "DATAFLOW"
    COMMIT = "COMMIT"
    SCHEDULE = "SCHEDULE"

class LoadType(Enum):
    INITIAL = "initial"
    DELTA = "delta"

class EventSet(BaseModel):
    on_schedule: Optional[list["Schedule"]] = None
    on_commit: Optional["CommitEvent"] = None
    on_dataflow: Optional[list[str]] = None
    dataflows: Optional[list["Dataflow"]] = None
    _triggers: Optional["Triggers"] = None

    def add_schedule(self, weekdays: Optional[list[Weekdays]] = None,
                 hours: Optional[list[int]] = None,
                 minutes: Optional[list[int]] = None,
                 days: Optional[list[int]] = None,
                 months: Optional[list[int]] = None,
                 last_day_of_month: Optional[bool] = None) -> None:
        """
        Run the function at these times, which are and' together, with the exception of the last_day_of_month.
        The latter has to be seen to add one entry to the days set with the day number of the last day of the current month.
        If no month/day is provided it means every month/day. If no hour/minute is provided it means at hour=0/minute=0.
        Example:
            days = {1, 15}; hours = {0, 12}; last_day_of_month = True
            means, it will execute on the 1st, the 15th and the 31st, two times each day
        
        :param weekdays: set of Weekdays
        :param hours: set of numbers between 0 and 23
        :param minutes: set of numbers between 0 and 59
        :param days: set of numbers between 1 and 31
        :param last_day_of_month: Should the last day of the current month be added to he days set?
        :param months: set of numbers between 1 and 12
        """
        if self.on_schedule is None:
            self.on_schedule = [
                Schedule(weekdays=[i.name for i in weekdays] if weekdays is not None else None, hours=hours, minutes=minutes, days=days, last_day_of_month=last_day_of_month, months=months)
            ]
        else:
            self.on_schedule.append(Schedule(weekdays=[i.name for i in weekdays] if weekdays is not None else None, hours=hours, minutes=minutes, days=days, last_day_of_month=last_day_of_month, months=months))
    
    def add_dataflow_event(self, dataflow_name: str) -> None:
        """
        This function should be called whenever a dataflow of a given name completed
        
        :param dataflow_name: The name of the dataflow to look for in the execution_log topic
        """
        if self.on_dataflow is None:
            self.on_dataflow = [dataflow_name]
        else:
            self.on_dataflow.append(dataflow_name)

    def set_commit_event(self, schema_names: list[str], topic_partitions: Optional[list[str]] = None, delay_seconds: Optional[int] = None, idle_seconds: Optional[int] = None) -> None:
        """
        Trigger this function whenever there is new data for a table name (=schema_name).
        
        :param schema_names: The name of the table/Kafka schema
        :param topic_partitions: But only if the data was for either of these partitions
        :param delay_seconds: Collect commits for this many seconds after the first was received to avoid frequent calls
        :param idle_seconds: collect commits until there were no more for some time
        """
        self.on_commit = CommitEvent(schema_names=schema_names, topic_partitions=topic_partitions, delay_seconds=delay_seconds, idle_seconds=idle_seconds)
    
    def add_dataflow(self, dataflow_name: str, partitions: Optional[list[int]] = None):
        """
        Invoke the function n-times, with each provided dataflow name and source partition as provided.
        Example: The function load-SAP-data should run at 04:00 and load the table MARA, MARC, MARD, MAKT.
        Because MAKT is so large, it is loaded with two parallel processes, each loading half the data, partition 0,1.
        
        :param dataflow_name: Tells the function which dataflow (on table = one dataflow probably) to execute
        :param partitions: In case the dataflow is using partitions, it must be invoked for each
        """
        if self.dataflows is None:
            self.dataflows = []
        self.dataflows.append(Dataflow(dataflow_name=dataflow_name, partitions=partitions))
    
    def get_triggers(self) -> Optional["Triggers"]:
        return self._triggers

    def __hash__(self) -> int:
        return hash(self._triggers.function_name) if self._triggers is not None else 0
        

class Triggers(BaseModel):

    function_name: str
    queuename: str
    events: list[EventSet] = list()

    def add_eventset(self) -> EventSet:
        eventset = EventSet(_triggers=self)
        self.events.append(eventset)
        return eventset

    @model_validator(mode='after')
    def post_deserialize(self) -> Self:
        for i in self.events:
            i._triggers = self
        return self

    def __repr__(self) -> str:
        return f"Triggers for function {self.function_name}"

    def __hash__(self) -> int:
        return hash(self.function_name)


class Schedule(BaseModel):

    weekdays: Optional[list[str]] = None
    hours: Optional[list[int]] = None
    minutes: Optional[list[int]] = None
    days: Optional[list[int]] = None
    last_day_of_month: Optional[bool] = None
    months: Optional[list[int]] = None

    def __repr__(self) -> str:
        return f"Schedule"


class CommitEvent(BaseModel):

    schema_names: list[str]
    topic_partitions: Optional[list[str]] = None
    delay_seconds: Optional[int] = None
    idle_seconds: Optional[int] = None
    
    def __repr__(self) -> str:
        return f"CommitEvent: {self.schema_names}"
    

class Dataflow(BaseModel):

    dataflow_name: str
    delta_single_trigger: Optional[bool] = None
    """
    If true, ignore partition list for delta loads, use partitions for initial loads only.
    """
    partitions: Optional[list[int]] = None
    """
    The scheduler will call the function for each partition provided. If no partitions are provided, it 
    will call the function once with no partition parameter.
    """

    def __repr__(self) -> str:
        return f"Dataflow: {self.dataflow_name}"


class TriggerSchema(ValueSchema):

    def __init__(self):
        super().__init__("triggers", None)

        schedule = avro_datatypes.RecordSchema("schedule", doc="all conditions within the fields must be met to run, they are AND conditions")
        schedule.add_field("weekdays", avro_datatypes.ArraySchema(avro_datatypes.AvroString()), doc="if provided, run only on these days, e.g. Mon-Fri", nullable=True)
        schedule.add_field("hours", avro_datatypes.ArraySchema(avro_datatypes.AvroInt()), doc="if provided, run only on these hours of the day", nullable=True)
        schedule.add_field("minutes", avro_datatypes.ArraySchema(avro_datatypes.AvroInt()), doc="if provided, run only on these minutes of the hours", nullable=True)
        schedule.add_field("days", avro_datatypes.ArraySchema(avro_datatypes.AvroInt()), doc="if provided, run only on these days of the month", nullable=True)
        schedule.add_field("last_day_of_month", avro_datatypes.AvroBoolean(), doc="an additional day to the days array", nullable=True)
        schedule.add_field("months", avro_datatypes.ArraySchema(avro_datatypes.AvroInt()), doc="if provided, run only on these months", nullable=True)

        commit_event = avro_datatypes.RecordSchema("on_commit", doc="trigger if a commit of that type was issued")
        commit_event.add_field("schema_names", avro_datatypes.ArraySchema(avro_datatypes.AvroString()), nullable=True, doc="trigger when the table with this schema name has a commit")
        commit_event.add_field("topic_partitions", avro_datatypes.ArraySchema(avro_datatypes.AvroString()), nullable=True, doc="only trigger if data was in any of these topic/partitions")
        commit_event.add_field("delay_seconds", avro_datatypes.AvroInt(), nullable=True, doc="if a first trigger event occured, wait this many seconds to collect more, thus avoiding frequent triggers")
        commit_event.add_field("idle_seconds", avro_datatypes.AvroInt(), nullable=True, doc="wait until no trigger event occured for this many seconds and only then start, to avoid frequent triggers")

        dataflows = avro_datatypes.RecordSchema("dataflow", doc="the function should be called for all these dataflows")
        dataflows.add_field("dataflow_name", avro_datatypes.AvroString(), nullable=False, doc="the dataflow name to pass in as parameter")
        dataflows.add_field("delta_single_trigger", avro_datatypes.AvroBoolean(), nullable=True, doc="If true, ignore partition list for delta loads, use partitions for initial loads only.")
        dataflows.add_field("partitions", avro_datatypes.ArraySchema(avro_datatypes.AvroInt()), nullable=True, doc="the partition parameters to use for this dataflow")

        events = avro_datatypes.RecordSchema("events", doc="a function can have different combinations of dataflows and events")
        events.add_field("on_commit", commit_event, 
                       doc="if provided, run only when a commit for this was found", 
                       nullable=True)
        events.add_field("on_dataflow", avro_datatypes.ArraySchema(avro_datatypes.AvroString()), 
                       doc="if provided, run when a dataflow completed", 
                       nullable=True)
        events.add_field("on_schedule", avro_datatypes.ArraySchema(schedule), 
                       doc="if provided, run only at the specified fixed times (in UTC); schedules and events are OR conditions", 
                       nullable=True)
        events.add_field("dataflows", avro_datatypes.ArraySchema(dataflows), 
                       doc="if provided, call the URL n times, once per dataflow or dataflow/partition", 
                       nullable=True)
        self.add_field("events", avro_datatypes.ArraySchema(events), nullable=True, doc="all combinations of dataflow and trigger events")

        self.add_field("function_name", avro_datatypes.AvroString(), nullable=False, doc="an arbitrary name, often the function or container name")
        self.add_field("queuename", avro_datatypes.AvroString(), 
                       doc="the queue name of the function", 
                       nullable=False)
        self.set_pks({"function_name",})


class ExecutionLogSchema(ValueSchema):

    def __init__(self):
        super().__init__("execution_log", None)
        self.add_field("function_name", avro_datatypes.AvroString(), nullable=False)
        self.add_field("dataflow_name", avro_datatypes.AvroString(), nullable=False)
        self.add_field("partition", avro_datatypes.AvroInt(), nullable=True)
        self.add_field("source_pointer", avro_datatypes.AvroString(), nullable=True)
        self.add_field("commit_id", avro_datatypes.AvroString(), nullable=True, doc="Transaction id of the last commit")
        self.add_field("load_type", avro_datatypes.AvroString(), nullable=True)
        self.add_field("id", avro_datatypes.AvroString(), nullable=True)
        self.add_field("called_by", avro_datatypes.AvroString(), nullable=True)
        self.add_field("called_reason", avro_datatypes.AvroString(), nullable=True)
        self.add_field("start_ts", avro_datatypes.AvroTimestampMicros(), nullable=False, default=0)
        self.add_field("row_count", avro_datatypes.AvroLong(), nullable=True)
        self.add_field("table_row_counts", avro_datatypes.AvroMap(avro_datatypes.AvroInt()), nullable=True, doc="The row counts per table/schema_name")
        self.set_pks({"producer_name","dataflow_name", "partition"})


class ExecutionLogEntry(BaseModel):

    id: str
    function_name: str
    dataflow_name: Optional[str] = None
    partition: Optional[int] = None
    source_pointer: Optional[str] = None
    commit_id: Optional[str] = None
    load_type: Optional[str] = None
    called_by: Optional[str] = None
    called_reason: Optional[str] = None
    create_date: Optional[datetime] = datetime.now(timezone.utc)
    start_ts: Optional[datetime] = None
    completed_date: Optional[datetime] = None
    state: ExecutionLogState = ExecutionLogState.STARTED
    row_count: Optional[int] = None
    queue_msg_id: Optional[str] = None

class QueueMessage(BaseModel):

    id: str
    load_type: str
    dataflow_name: Optional[str]
    partition: Optional[int]
    commit_id: Optional[str] = None
    delta_pointer: str
    called_by: str
    caller_reason: str

    def get_caller_reason(self) -> CalledReason:
        return CalledReason(self.caller_reason)
