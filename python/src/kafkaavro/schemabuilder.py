import jsonpickle
from typing import Optional

import pyarrow

from .data_governance import FKCondition, Duration, DeletionPolicy
from .avro_datatypes import AvroString, AvroVarchar, AvroNVarchar, AvroByte, AvroMap, \
    AvroTimestamp, RecordSchema, ArraySchema, AvroTimestampMicros, AvroLong, AvroInt, AvroBoolean

ROW_SOURCE_SYSTEM = "__source_system"
ROW_SOURCE_TRANSACTION = "__source_transaction"
ROW_RECORD_ID = "__source_rowid"
ROW_CHANGE_TS = "__change_time"
ROW_TYPE_FIELD = "__change_type"
ROW_TRUNCATE = "__truncate"
SCHEMA_COLUMN_EXTENSION = "__extension"
SCHEMA_INFO_TICKETS_URL = "tickets_url"
SCHEMA_INFO_REPO_URL = "repo_url"
SCHEMA_INFO_DATAPRODUCT_OWNER = "data_product_owner_email"

extension = RecordSchema(SCHEMA_COLUMN_EXTENSION, doc="Extension point to add custom values to each record")
extension.add_field("__path", AvroString(), 'An unique identifier, e.g. "street"."house number component"',
                    False)
extension.add_field("__value", AvroString(),"The value of any primitive datatype of Avro",
                    False)

class RootSchema(RecordSchema):
    """
    The Kafka key should be a RootSchema structure
    """

    def __init__(self, name: str, namespace: Optional[str]):
        super().__init__(name, namespace)

    def create_schema_dict(self) -> dict[str, any]:
        schema_data = super().create_schema_dict()
        return schema_data

    def get_json(self) -> str:
        return jsonpickle.dumps(self.create_schema_dict())

    def get_pyarrow(self) -> pyarrow.Schema:
        f = [(field.name, field.type.get_pyarrow()) for field in self.fields]
        return pyarrow.schema(f)


class ValueSchema(RootSchema):
    """
    The valueSchema is the schema used for the Kafka payload.
    It contains lots of data governance information provided by the developer of the process
    """

    def __init__(self, name: str, namespace: Optional[str]):
        super().__init__(name, namespace)

        audit_details_record = RecordSchema("__audit_details", None)
        audit_details_record.add_field("__transformationname", AvroNVarchar(1024),
                                       doc="A name identifying the applied transformation")
        audit_details_record.add_field("__transformresult", AvroVarchar(4),
                                       doc="Is the record PASS, FAIL or WARN?")
        audit_details_record.add_field("__transformresult_text", AvroNVarchar(1024),
                                       doc="Transforms can optionally describe what they did")
        audit_details_record.add_field("__transformresult_quality", AvroByte(),
                                       doc="Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)")

        details_array = ArraySchema(audit_details_record)
        audit_record = RecordSchema("__audit", None)
        audit_record.add_field("__transformresult", AvroVarchar(4),
                               doc="Is the record PASS, FAIL or WARN?")
        audit_record.add_field("__details", details_array,
                               doc="Details of all transformations")

        self.add_field("__audit", audit_record)
        self.add_field(ROW_TYPE_FIELD, AvroVarchar(1), internal=True, technical=True,
                       doc="Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate,...")
        self.add_field(ROW_CHANGE_TS, AvroTimestamp(), internal=True, technical=True,
                       doc="Timestamp of the transaction. All rows of the transaction have the same value.")
        self.add_field(ROW_RECORD_ID, AvroVarchar(30), internal=True, technical=True,
                       doc="Optional unique and static pointer to the row, e.g. Oracle rowid")
        self.add_field(ROW_SOURCE_TRANSACTION, AvroVarchar(30), internal=True, technical=True,
                       doc="Optional source transaction information for auditing")
        self.add_field(ROW_SOURCE_SYSTEM, AvroVarchar(30), internal=True, technical=True,
                       doc="Optional source system information for auditing")
        self.add_field(ROW_TRUNCATE, AvroMap(AvroString()),
                       doc="In case of a change type of TRUNCATE, this map contains the fields to identify the set of rows to be deleted")
        self.add_field(SCHEMA_COLUMN_EXTENSION, extension, internal=True, doc="Add more columns beyond the official logical data model")

        self.pks = None # type: Optional[set[str]]
        self.fks = None # type: Optional[list[FKCondition]]
        self.source_system_uri = None # type: Optional[str]
        self.data_product_owner_email = None # type: Optional[str]
        self.retention_period = None # type: Optional[Duration]
        self.deletion_policy = None # type: Optional[DeletionPolicy]
        self.data_classifications = None # type: Optional[set[str]]
        self.repo_url = None # type: Optional[str]
        self.tickets_url = None # type: Optional[str]

    def create_schema_dict(self) -> dict[str, any]:
        schema_data = super().create_schema_dict()
        schema_data["pks"] = self.pks
        schema_data["fks"] = self.fks
        schema_data[SCHEMA_INFO_DATAPRODUCT_OWNER] = self.data_product_owner_email
        schema_data['retention_period'] = self.retention_period
        schema_data['deletion_policy'] = self.deletion_policy
        schema_data['data_classifications'] = self.data_classifications
        schema_data[SCHEMA_INFO_TICKETS_URL] = self.tickets_url
        schema_data[SCHEMA_INFO_REPO_URL] = self.repo_url
        return schema_data

    def set_pks(self, pk_columns: Optional[set[str]]) -> "ValueSchema":
        self.pks = pk_columns
        return self

    def add_pk(self, column: str) -> "ValueSchema":
        if self.pks is None:
            self.pks = set()
        self.pks.add(column)
        return self

    def remove_pk(self, column: str) -> "ValueSchema":
        if self.pks is not None:
            self.pks.remove(column)
        return self

    def set_fks(self, fks: Optional[list[FKCondition]]) -> "ValueSchema":
        self.fks = fks
        return self

    def add_fk(self, condition: FKCondition) -> "ValueSchema":
        if self.fks is None:
            self.fks = []
        self.fks.append(condition)
        return self
    
    def set_repo_url(self, url: Optional[str]) -> "ValueSchema":
        self.repo_url = url
        return self
    
    def set_tickets_url(self, url: Optional[str]) -> "ValueSchema":
        self.tickets_url = url
        return self
    
    def set_source_system_uri(self, uri: Optional[str]) -> "ValueSchema":
        self.source_system_uri = uri
        return self

    def set_data_product_owner_email(self, email: Optional[str]) -> "ValueSchema":
        self.data_product_owner_email = email
        return self

    def set_retention_period(self, period: Optional[Duration]) -> "ValueSchema":
        self.retention_period = period
        return self

    def set_deletion_policy(self, policy: Optional[DeletionPolicy]) -> "ValueSchema":
        self.deletion_policy = policy
        return self

    def add_data_classifications(self, data_classification: str) -> "ValueSchema":
        if self.data_classifications is None:
            self.data_classifications = set()
        self.data_classifications.add(data_classification)
        return self

    def remove_data_classifications(self, data_classification: str) -> "ValueSchema":
        if self.data_classifications is not None:
            self.data_classifications.remove(data_classification)
        return self


class KeySchema(RootSchema):
    """
    Derive the key schema from the ValueSchema
    """
    def __init__(self, value_schema: ValueSchema):
        super().__init__(f"{value_schema.name}_key", value_schema.namespace)
        if value_schema.pks is None or len(value_schema.pks) == 0:
            self.add_field("ts", AvroTimestampMicros(), None, False)
        else:
            for pk_column in value_schema.pks:
                f = value_schema.field_name_index[pk_column]
                self.add_field(f.name, f.type, f.doc, False, f.internal, f.technical, f.source_data_type, f.default)

class CommitSchema(ValueSchema):

    def __init__(self):
        super().__init__("commit", None)

        offset = RecordSchema("min_max_offsets", None)
        offset.add_field("min_offset", AvroLong())
        offset.add_field("max_offset", AvroLong())
        offset.add_field("partition", AvroInt())

        topic = RecordSchema("topic_offsets", None)
        topic.add_field("topic_name", AvroString())
        topic.add_field("schema_names", ArraySchema(AvroString()))
        topic.add_field("offsets", AvroMap(offset))

        self.add_field("commit_id", AvroString())
        self.add_field("producer_name", AvroString(), default="")
        self.add_field("commit_epoch_ns", AvroLong())
        self.add_field("record_count", AvroInt(), default=0)
        self.add_field("rollback", AvroBoolean(), default=False)
        self.add_field("topics", AvroMap(topic), nullable=True)
        self.set_pks({"commit_id", "producer_name"})


class CommitSchemaKey(RootSchema):

    def __init__(self):
        super().__init__("commit_key", None)
        self.add_field("commit_id", AvroString())
        self.add_field("producer_name", AvroString(), default="")

