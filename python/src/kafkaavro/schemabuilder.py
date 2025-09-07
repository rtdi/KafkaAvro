import jsonpickle
from typing import Union, Optional
import re
import pyarrow

from .data_governance import FKCondition, DataSensitivityEnum, Duration, DeletionPolicy
from .avro_datatypes import AvroPrimitive, AvroString, AvroAnyPrimitive, AvroVarchar, AvroNVarchar, AvroByte, AvroMap, \
    AvroTimestamp

ROW_SOURCE_SYSTEM = "__source_system"
ROW_SOURCE_TRANSACTION = "__source_transaction"
ROW_RECORD_ID = "__source_rowid"
ROW_CHANGE_TS = "__change_time"
ROW_TYPE_FIELD = "__change_type"
ROW_TRUNCATE = "__truncate"
COLUMN_PROP_SOURCE_DATATYPE = "__source_data_type"
COLUMN_PROP_ORIGINAL_NAME = "__originalname"
COLUMN_PROP_INTERNAL = "__internal"
COLUMN_PROP_TECHNICAL = "__technical" # cannot be used in mappings as the values are set when sending the rows to the pipeline server
COLUMN_PROP_CONTENT_SENSITIVITY = "__sensitivity"
SCHEMA_COLUMN_EXTENSION = "__extension"
SCHEMA_INFO_TICKETS_URL = "tickets_url"
SCHEMA_INFO_REPO_URL = "repo_url"
SCHEMA_INFO_DATAPRODUCT_OWNER = "data_product_owner_email"

encoder_pattern = re.compile(r'[^A-Za-z0-9_.]')  # dots are allowed also
decoder_pattern = re.compile(r'_x[0-9a-fA-F]{4}')

def encode_name(s: str) -> str:
    # Escape the literal "_x" to avoid confusion with escape sequences
    s = s.replace('_x', '_x005f_x0078')
    buf = []
    last_pos = 0
    matches = re.finditer(encoder_pattern, s)
    for match in matches:
        start, end = match.span()
        buf.append(s[last_pos:start])
        char = match.group()
        encoded = f"_x{ord(char):04x}"
        buf.append(encoded)
        last_pos = end

    buf.append(s[last_pos:])
    return ''.join(buf)

def decode_name(name: str) -> str:
    def replace_match(m):
        hex_code = m.group()[2:]  # strip '_x'
        return chr(int(hex_code, 16))
    return decoder_pattern.sub(replace_match, name)


def get_key_schema(value_schema: "ValueSchema") -> "RootSchema":
    key_schema = RootSchema(value_schema.name, value_schema.namespace)
    if value_schema.pks is not None and len(value_schema.pks) > 0:
        for pk in value_schema.pks:
            f = value_schema.field_name_index.get(pk)
            if f is not None:
                key_schema.add_field(f.name, f.type, f.doc, f.nullable, f.internal, f.technical, f.source_data_type)
            else:
                raise RuntimeError(f"Primary key with name {pk} not found in list of fields of the value schema")
    else:
        raise RuntimeError(f"Value schema has no primary key defined")
    return key_schema


class Field:
    """
    A Field contains detailed metadata about the column, technical metadata and business metadata
    """

    def __init__(self, name: str, datatype: Union[AvroPrimitive, 'ArraySchema', 'RecordSchema'], nullable: bool = True, doc: str = None,
                 internal: bool = False, technical: bool = False, source_data_type: str = None):
        self.name = name # type: str
        self.type = datatype # type: Union[AvroPrimitive, 'AvroArray', 'RecordSchema']
        self.nullable = nullable # type: bool
        self.doc = doc # type: Optional[str]
        self.data_sensitivity: DataSensitivityEnum = DataSensitivityEnum.INTERNAL
        self.internal: bool = internal
        self.technical: bool = technical
        self.source_data_type: Optional[str] = source_data_type
        

    def create_schema_dict(self) -> dict[str, str]:
        s = dict()
        s['name'] = encode_name(self.name)
        s[COLUMN_PROP_ORIGINAL_NAME] = self.name
        s['doc'] = self.doc
        s[COLUMN_PROP_CONTENT_SENSITIVITY] = self.data_sensitivity.name
        s[COLUMN_PROP_INTERNAL] = self.internal
        s[COLUMN_PROP_TECHNICAL] = self.technical
        s[COLUMN_PROP_SOURCE_DATATYPE] = self.source_data_type
        if self.nullable:
            s['type'] = ["null", self.type.create_schema_dict()]
            s['default'] = None
        else:
            s['type'] = self.type.create_schema_dict()
        return s

    def set_data_sensitivity(self, data_sensitivity: DataSensitivityEnum) -> "Field":
        self.data_sensitivity = data_sensitivity
        return self

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(name={self.name!r}, type={self.type!r})"


class RecordSchema:

    def __init__(self, name: str, namespace: Optional[str] = None, doc: Optional[str] = None):
        self.type = 'record' # type: str
        self.name = name # type: str
        self.fields = [] # type: list[Field]
        self.namespace = namespace # type: Optional[str]
        self.doc = doc # type: Optional[str]
        self.field_name_index = {} # type: dict[str, Field]

    def add_field(self, name: str, datatype: any, doc: str = None, nullable: bool = True,
                  internal: bool = False, technical: bool = False, source_data_type: str = None) -> Field:
        f = Field(name, datatype, nullable, doc, internal, technical, source_data_type)
        self.fields.append(f)
        self.field_name_index[name] = f
        return f

    def create_schema_dict(self) -> dict[str, any]:
        s = dict()
        s['name'] = encode_name(self.name)
        s[COLUMN_PROP_ORIGINAL_NAME] = self.name
        s['doc'] = self.doc
        s['type'] = self.type
        s['namespace'] = self.namespace
        s['fields'] = [field.create_schema_dict() for field in self.fields]
        return s

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(name={self.name!r}, namespace={self.namespace!r}, field_count={len(self.fields)})"

    def get_pyarrow(self) -> any:
        f = [(field.name, field.type.get_pyarrow()) for field in self.fields]
        return pyarrow.struct(f)


extension = RecordSchema(SCHEMA_COLUMN_EXTENSION, doc="Extension point to add custom values to each record")
extension.add_field("__path", AvroString(), 'An unique identifier, e.g. "street"."house number component"',
                    False)
extension.add_field("__value", AvroAnyPrimitive(),"The value of any primitive datatype of Avro",
                    False)

class ArraySchema:

    def __init__(self, items: Union[AvroPrimitive, RecordSchema]):
        self.items = items

    def create_schema_dict(self) -> dict[str, any]:
        return {
            "type": "array",
            "items": self.items.create_schema_dict()
        }

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(items={self.items!r})"

    def get_pyarrow(self) -> any:
        return pyarrow.list_(self.items.get_pyarrow())


class RootSchema(RecordSchema):
    """
    The Kafka key should be a RootSchema structure
    """

    def __init__(self, name: str, namespace: Optional[str]):
        super().__init__(name, namespace)

    def create_schema_dict(self) -> dict[str, any]:
        schema_data = super().create_schema_dict()
        return schema_data

    def get_json(self):
        return jsonpickle.dumps(self.create_schema_dict())


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
                       doc="Optional unqiue and static pointer to the row, e.g. Oracle rowid")
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


commit_value_schema = """
        {
            "name": "commit",
            "type": "record",
            "pks": ["commit_id", "producer_name"],
            "fields": [
                {
                    "name": "commit_id",
                    "type": "string"
                },
                {
                    "name": "producer_name",
                    "type": "string",
                    "default": ""
                },
                {
                    "name": "commit_epoch_ns",
                    "type": "long"
                },
                {
                    "name": "record_count",
                    "type": "int",
                    "default": 0 
                },
                {
                    "name": "topics",
                    "type": [
                        "null",
                        {
                            "type": "map",
                            "values" : {
                                "name": "topic_offsets",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "topic_name",
                                        "type": "string"
                                    },
                                    {
                                        "name": "schema_names",
                                        "type": {
                                            "type": "array",
                                            "items" : "string"
                                        }
                                    },
                                    {
                                        "name": "offsets",
                                        "type": [
                                            {
                                                "type": "map",
                                                "values": {
                                                    "name": "min_max_offsets",
                                                    "type": "record",
                                                    "fields": [
                                                        {
                                                            "name": "min_offset",
                                                            "type": "long"
                                                        },
                                                        {
                                                            "name": "max_offset",
                                                            "type": "long"
                                                        },
                                                        {
                                                            "name": "partition",
                                                            "type": "int"
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    """
commit_key_schema = """
        {
            "name": "commit-key",
            "type": "record",
            "fields":
                [
                    {
                        "name": "commit_id",
                        "type": "string"
                    },
                    {
                        "name": "producer_name",
                        "type": "string"
                    }
                ]
        }           
    """

