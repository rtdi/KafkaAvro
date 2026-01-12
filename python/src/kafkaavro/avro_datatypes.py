from datetime import timezone
from typing import Union, Optional, Any
import re

import jsonpickle
import pyarrow
from abc import ABC, abstractmethod

from .data_governance import DataSensitivityEnum

encoder_pattern = re.compile(r'[^A-Za-z0-9_]')
decoder_pattern = re.compile(r'_x[0-9a-fA-F]{4}')

COLUMN_PROP_SOURCE_DATATYPE = "__source_data_type"
COLUMN_PROP_ORIGINAL_NAME = "__originalname"
COLUMN_PROP_INTERNAL = "__internal"
COLUMN_PROP_TECHNICAL = "__technical" # cannot be used in mappings as the values are set when sending the rows to the pipeline server
COLUMN_PROP_CONTENT_SENSITIVITY = "__sensitivity"

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

class AvroPrimitive(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def create_schema_dict(self) -> dict:
        pass

    @abstractmethod
    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.null()


class Field:
    """
    A Field contains detailed metadata about the column, technical metadata and business metadata
    """

    def __init__(self, name: str, datatype: Union[AvroPrimitive, 'ArraySchema', 'RecordSchema'], nullable: bool = True,
                 doc: str = None,
                 internal: bool = False, technical: bool = False, source_data_type: str = None, default: Any = None):
        self.name = name  # type: str
        self.type = datatype  # type: Union[AvroPrimitive, 'AvroArray', 'RecordSchema']
        self.nullable = nullable  # type: bool
        self.doc = doc  # type: Optional[str]
        self.data_sensitivity: DataSensitivityEnum = DataSensitivityEnum.INTERNAL
        self.internal: bool = internal
        self.technical: bool = technical
        self.source_data_type: Optional[str] = source_data_type
        self.default = default

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
            s['default'] = None # The default of a nullable is null
        else:
            s['type'] = self.type.create_schema_dict()
            if self.default is not None:
                s['default'] = self.default # if the field is not nullable, the default cannot be null
        return s

    def set_data_sensitivity(self, data_sensitivity: DataSensitivityEnum) -> "Field":
        self.data_sensitivity = data_sensitivity
        return self

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(name={self.name!r}, type={self.type!r})"


class AvroUnion:

    def __init__(self, items: list[AvroPrimitive]):
        self.items = items

    def create_schema_dict(self) -> Union[list, dict[str, any]]:
        return [i.create_schema_dict() for i in self.items]

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(items={self.items!r})"

    def get_pyarrow(self) -> any:
        l = []
        for i in self.items:
            l.append(pyarrow.field(type(i).__name__, i.get_pyarrow()))
        return pyarrow.union(l, pyarrow.lib.UnionMode_DENSE)

class RecordSchema:

    def __init__(self, name: str, namespace: Optional[str] = None, doc: Optional[str] = None):
        self.type = 'record' # type: str
        self.name = name # type: str
        self.fields = [] # type: list[Field]
        self.namespace = namespace # type: Optional[str]
        self.doc = doc # type: Optional[str]
        self.field_name_index = {} # type: dict[str, Field]

    def add_field(self, name: str, datatype: any, doc: Optional[str] = None, nullable: bool = True,
                  internal: bool = False, technical: bool = False, source_data_type: str = None, default: Any = None) -> Field:
        f = Field(name, datatype, nullable, doc, internal, technical, source_data_type, default)
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

class AvroAnyPrimitive(AvroPrimitive):

    def __init__(self):
        super().__init__()
        items = [
            AvroBoolean(),
            AvroBytes(),
            AvroDouble(),
            AvroFloat(),
            AvroInt(),
            AvroLong(),
            AvroString()
        ]
        self.union = AvroUnion(items)

    def create_schema_dict(self) -> dict:
        return self.union.create_schema_dict()

    def get_pyarrow(self) -> pyarrow.DataType:
        return self.union.get_pyarrow()


class AvroBoolean(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "boolean", "logicalType": "BOOLEAN"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.bool_()


class AvroByte(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "int", "logicalType": "BYTE"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int8()


class AvroBytes(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "bytes", "logicalType": "BYTES"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.large_binary()


class AvroCLOB(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "CLOB"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroDate(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "int", "logicalType": "date"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.date32()


class AvroDecimal(AvroPrimitive):

    def __init__(self, precision: int, scale: int):
        super().__init__()
        self.precision = precision
        self.scale = scale

    def create_schema_dict(self) -> dict:
        return {"type": "bytes", "logicalType": "decimal", "precision": self.precision, "scale": self.scale}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.decimal64(self.precision, self.scale)


class AvroDouble(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "double", "logicalType": "DOUBLE"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.float64()


class AvroEnum(AvroPrimitive):

    def __init__(self, symbols: Union[list[str], str], name: str):
        super().__init__()
        if isinstance(symbols, str):
            self.symbols = jsonpickle.decode(symbols)
        self.symbols = symbols
        self.name = name

    def create_schema_dict(self) -> dict:
        return {"type": "enum", "logicalType": "ENUM", "symbols": self.get_symbol_json(), "name": self.name}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()

    def get_symbol_json(self):
        return '["' + '","'.join(self.symbols) + '"]'


class AvroFixed(AvroPrimitive):

    def __init__(self, length: int, name: str):
        super().__init__()
        self.length = length
        self.name = name

    def create_schema_dict(self) -> dict:
        return {"type": "fixed", "logicalType": "FIXED", "size": self.length, "name": self.name}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.binary(self.length)


class AvroFloat(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "float", "logicalType": "FLOAT"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.float32()


class AvroInt(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "int", "logicalType": "INT"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int32()


class AvroLocalTimestamp(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "long", "logicalType": "local-timestamp-millis"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("ms", tz=timezone.utc)


class AvroLocalTimestampMicros(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "long", "logicalType": "local-timestamp-micros"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("us", tz=timezone.utc)


class AvroLong(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "long", "logicalType": "LONG"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int64()


class AvroMap(AvroPrimitive):

    def __init__(self, value_data_type: Union[AvroPrimitive, RecordSchema]):
        super().__init__()
        self.value_data_type = value_data_type

    def create_schema_dict(self) -> dict:
        return {"type": "map", "logicalType": "MAP", "values": self.value_data_type.create_schema_dict()}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.map_(pyarrow.string(), self.value_data_type.get_pyarrow())


class AvroNCLOB(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "NCLOB"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroNVarchar(AvroPrimitive):

    def __init__(self, length: int):
        super().__init__()
        self.length = length

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "NVARCHAR", "length": self.length}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroNull(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "null", "logicalType": "NULL"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.null()


class AvroSTGeometry(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "ST_GEOMETRY"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroSTPoint(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "ST_POINT"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroShort(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "int", "logicalType": "SHORT"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int16()


class AvroString(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "STRING"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroTime(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "int", "logicalType": "time-millis"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.time32("ms")


class AvroTimeMicros(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "long", "logicalType": "time-micros"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.time64("us")


class AvroTimestamp(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "long", "logicalType": "timestamp-millis"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("ms", tz=timezone.utc)


class AvroTimestampMicros(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "long", "logicalType": "timestamp-micros"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("us", tz=timezone.utc)


class AvroUUID(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "fixed", "size": 16, "logicalType": "uuid"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.uuid()


class AvroUri(AvroPrimitive):

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "URI"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroVarchar(AvroPrimitive):

    def __init__(self, length: int):
        super().__init__()
        self.length = length

    def create_schema_dict(self) -> dict:
        return {"type": "string", "logicalType": "VARCHAR", "length": self.length}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()



def get_datatype(schema: dict):
    t = schema["type"]
    if isinstance(t, dict):
        return get_datatype(t)
    if isinstance(t, list):
        # Union
        if len(t) == 2 and (t[0] == "null" or t[1] == "null"):
            if t[0] == "null":
                datatype, nullable = get_datatype(t[1])
                return datatype, True
            else:
                datatype, nullable = get_datatype(t[0])
                return datatype, True
        else:
            items = [get_datatype(i) for i in t]
            return AvroUnion(items), False
    match t:
        case "record":
            r = RecordSchema(schema["name"], schema.get("namespace"), schema.get("doc"))
            for f in schema["fields"]:
                datatype, nullable = get_datatype(f)
                field = r.add_field(f["name"], datatype, f.get("doc"), nullable,
                                    f.get(COLUMN_PROP_INTERNAL),
                                    f.get(COLUMN_PROP_TECHNICAL),
                                    f.get(COLUMN_PROP_SOURCE_DATATYPE))
                s = f.get(COLUMN_PROP_SOURCE_DATATYPE)
                if s is not None:
                    field.set_data_sensitivity(DataSensitivityEnum(s))
            return r, False
        case "array":
            datatype, nullable = get_datatype(schema["items"])
            return ArraySchema(datatype), False
    l = schema.get("logicalType")
    if l is not None:
        match l:
            case "BOOLEAN":
                return AvroBoolean(), False
            case "BYTE":
                return AvroByte(), False
            case "BYTES":
                return AvroBytes(), False
            case "CLOB":
                return AvroCLOB, False
            case "date":
                return AvroDate(), False
            case "decimal":
                return AvroDecimal(schema.get("precision"), schema.get("scale")), False
            case "DOUBLE":
                return AvroDouble(), False
            case "ENUM":
                return AvroEnum(schema.get("symbols"), schema.get("name")), False
            case "FIXED":
                return AvroFixed(schema.get("size"), schema.get("name")), False
            case "FLOAT":
                return AvroFloat(), False
            case "INT":
                return AvroInt(), False
            case "local-timestamp-millis":
                return AvroLocalTimestamp(), False
            case "local-timestamp-micros":
                return AvroLocalTimestampMicros(), False
            case "LONG":
                return AvroLong(), False
            case "MAP":
                d, n = get_datatype(schema.get("values"))
                return AvroMap(d), False
            case "NCLOB":
                return AvroNCLOB(), False
            case "NULL":
                return AvroNull(), False
            case "NVARCHAR":
                return AvroNVarchar(schema.get("length")), False
            case "ST_GEOMETRY":
                return AvroSTGeometry(), False
            case "ST_POINT":
                return AvroSTPoint(), False
            case "SHORT":
                return AvroShort(), False
            case "STRING":
                return AvroString(), False
            case "time-millis":
                return AvroTime(), False
            case "time-micros":
                return AvroTimeMicros(), False
            case "timestamp-millis":
                return AvroTimestamp(), False
            case "timestamp-micros":
                return AvroTimestampMicros(), False
            case "uuid":
                return AvroUUID(), False
            case "URI":
                return AvroUri(), False
            case "VARCHAR":
                return AvroVarchar(schema.get("length")), False
        match t:
            case "null":
                return AvroNull(), False
            case "boolean":
                return AvroBoolean(), False
            case "int":
                return AvroInt(), False
            case "long":
                return AvroLong(), False
            case "float":
                return AvroFloat(), False
            case "double":
                return AvroDouble(), False
            case "bytes":
                return AvroBytes(), False
            case "string":
                return AvroString(), False
            case "enum":
                return AvroEnum(schema.get("symbols"), schema.get("name")), False
            case "map":
                d, n = get_datatype(schema.get("values"))
                return AvroMap(d), False
            case "fixed":
                return AvroFixed(schema.get("size"), schema.get("name")), False
        raise RuntimeError(f"unknown schema {schema}")