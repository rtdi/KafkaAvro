from typing import Optional, Any

import pyarrow
from pydantic import BaseModel

from .data_governance_pydantic import FKCondition, Duration, DeletionPolicy
from .avro_datatypes_pydantic import AvroString, AvroVarchar, AvroNVarchar, AvroByte, AvroMap, \
    AvroTimestamp, RecordSchema, ArraySchema, AvroTimestampMicros, AvroLong, AvroInt, AvroBoolean
from .table_constants import ROW_SOURCE_SYSTEM, ROW_SOURCE_TRANSACTION, ROW_RECORD_ID, ROW_CHANGE_TS, ROW_TYPE_FIELD, \
    ROW_TRUNCATE, SCHEMA_COLUMN_EXTENSION, TableType, AUDIT

extension = RecordSchema(name=SCHEMA_COLUMN_EXTENSION, doc="Extension point to add custom values to each record")
extension.add_field("__path", AvroString(), 'An unique identifier, e.g. "street"."house number component"',
                    False)
extension.add_field("__value", AvroString(),"The value of any primitive datatype of Avro",
                    False)
audit_details_record = RecordSchema(name="__audit_details", namespace=None)
audit_details_record.add_field("__transformationname", AvroNVarchar(length=1024),
                               doc="A name identifying the applied transformation")
audit_details_record.add_field("__transformresult", AvroVarchar(length=4),
                               doc="Is the record PASS, FAIL or WARN?")
audit_details_record.add_field("__transformresult_text", AvroNVarchar(length=1024),
                               doc="Transforms can optionally describe what they did")
audit_details_record.add_field("__transformresult_quality", AvroByte(),
                               doc="Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)")

details_array = ArraySchema(items=audit_details_record)
audit_record = RecordSchema(name=AUDIT, namespace=None)
audit_record.add_field("__transformresult", AvroVarchar(length=4),
                       doc="Is the record PASS, FAIL or WARN?")
audit_record.add_field("__details", details_array,
                       doc="Details of all transformations")


class RootSchema(RecordSchema):
    """
    The Kafka key should be a RootSchema structure
    """

    def get_json(self) -> str:
        return self.model_dump_json()

    def get_pyarrow(self) -> pyarrow.Schema:
        f = [(field.name, field.type.get_pyarrow()) for field in self.fields]
        return pyarrow.schema(f)

class RLS(BaseModel):
    """
    Rowlevel security is implemented using a permission table. That table has a structure like
    | dimension | value | username  |
    +-----------+-------+-----------+
    | region    | US    | user1     |
    | region    | EMEA  | user2     |

    The data table will be joined with the permission table like
    select * from SALES
    where SALES_REGION in (select value from permission_table where username=user() and dimension = <dimension>)
    """
    dimension: str
    """
    The dimension name to be used in the permission table
    """
    field: str
    """
    The field name in this schema that holds the dimension's value, e.g. data_table's SALES_REGION column
    """


class TableSemantic(BaseModel):
    type: TableType
    """
    What is the main purpose of this table? Fact, dimension,... 
    """

class ValueSchema(RootSchema):
    """
    The valueSchema is the schema used for the Kafka payload.
    It contains lots of data governance information provided by the developer of the process
    """

    pks: Optional[list[str]] = None
    fks: Optional[list[FKCondition]] = None
    source_system_uri: Optional[str] = None
    data_product_owner_email: Optional[str] = None
    retention_period: Optional[Duration] = None
    deletion_policy: Optional[DeletionPolicy] = None
    data_classifications: Optional[list[str]] = None
    repo_url: Optional[str] = None
    tickets_url: Optional[str] = None
    object_level_security: Optional[list[str]] = None
    row_level_security: Optional[list[RLS]] = None
    partition_by: Optional[list[str]] = None
    semantics: Optional[TableSemantic] = None

    def set_semantic(self, table_type: TableType):
        self.semantics = TableSemantic(type=table_type)

    def model_post_init(self, context: Any) -> None:
        for f in self.fields:
            if isinstance(f.type, RecordSchema) and f.name == AUDIT:
                return
        self.add_field(AUDIT, audit_record, internal=True, technical=True)
        self.add_field(ROW_TYPE_FIELD, AvroVarchar(length=1), internal=True, technical=True,
                       doc="Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate,...")
        self.add_field(ROW_CHANGE_TS, AvroTimestamp(), internal=True, technical=True,
                       doc="Timestamp of the transaction. All rows of the transaction have the same value.")
        self.add_field(ROW_RECORD_ID, AvroVarchar(length=30), internal=True, technical=True,
                       doc="Optional unique and static pointer to the row, e.g. Oracle rowid")
        self.add_field(ROW_SOURCE_TRANSACTION, AvroVarchar(length=30), internal=True, technical=True,
                       doc="Optional source transaction information for auditing")
        self.add_field(ROW_SOURCE_SYSTEM, AvroVarchar(length=30), internal=True, technical=True,
                       doc="Optional source system information for auditing")
        self.add_field(ROW_TRUNCATE, AvroMap(values=AvroString()),
                       doc="In case of a change type of TRUNCATE, this map contains the fields to identify the set of rows to be deleted",
                       internal=True, technical=True)
        self.add_field(SCHEMA_COLUMN_EXTENSION, extension, internal=True, doc="Add more columns beyond the official logical data model")

    def set_object_level_security(self, groups: Optional[list[str]]):
        self.object_level_security = groups

    def set_row_level_security(self, rls: Optional[list[RLS]]):
        self.row_level_security = rls

    def set_partition_by(self, partitions: Optional[list[str]]):
        self.partition_by = partitions

    def set_pks(self, pk_columns: Optional[set[str]]) -> "ValueSchema":
        if pk_columns is not None:
            self.pks = list(pk_columns)
        else:
            self.pks = None
        return self

    def add_pk(self, column: str) -> "ValueSchema":
        if self.pks is None:
            self.pks = list()
        self.pks.append(column)
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
            self.data_classifications = list()
        self.data_classifications.append(data_classification)
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
        super().__init__(name=f"{value_schema.name}_key", namespace=value_schema.namespace)
        if value_schema.pks is None or len(value_schema.pks) == 0:
            self.add_field("ts", AvroTimestampMicros(), None, False)
        else:
            for pk_column in value_schema.pks:
                f = value_schema._field_name_index[pk_column]
                self.add_field(f.name, f.type, f.doc, False, f.internal, f.technical, f.source_data_type, f.default)

class CommitSchema(ValueSchema):

    def __init__(self):
        super().__init__(name="commit", namespace=None)

        offset = RecordSchema(name="min_max_offsets", namespace=None)
        offset.add_field("min_offset", AvroLong())
        offset.add_field("max_offset", AvroLong())
        offset.add_field("partition", AvroInt())

        topic = RecordSchema(name="topic_offsets", namespace=None)
        topic.add_field("topic_name", AvroString())
        topic.add_field("schema_names", ArraySchema(items=AvroString()))
        topic.add_field("offsets", AvroMap(values=offset))

        self.add_field("commit_id", AvroString())
        self.add_field("producer_name", AvroString(), default="")
        self.add_field("commit_epoch_ns", AvroLong())
        self.add_field("record_count", AvroInt(), default=0)
        self.add_field("rollback", AvroBoolean(), default=False)
        self.add_field("topics", AvroMap(values=topic), nullable=True)
        self.set_pks({"commit_id", "producer_name"})


class CommitSchemaKey(RootSchema):

    def __init__(self):
        super().__init__(name="commit_key", namespace=None)
        self.add_field("commit_id", AvroString())
        self.add_field("producer_name", AvroString(), default="")

