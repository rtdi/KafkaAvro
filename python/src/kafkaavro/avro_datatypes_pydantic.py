from datetime import timezone
from enum import Enum
from typing import Union, Optional, Any, Literal, Annotated, Self
import re

import pyarrow
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field as Pydantic_Field, Tag, Discriminator, \
    field_serializer, model_serializer, ConfigDict, model_validator, RootModel
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from .data_governance import DataSensitivityEnum

encoder_pattern = re.compile(r'[^A-Za-z0-9_]')
decoder_pattern = re.compile(r'_x[0-9a-fA-F]{4}')

COLUMN_PROP_SOURCE_DATATYPE = "__source_data_type"
COLUMN_PROP_ORIGINAL_NAME = "__originalname"
COLUMN_PROP_INTERNAL = "__internal"
COLUMN_PROP_TECHNICAL = "__technical" # cannot be used in mappings as the values are set when sending the rows to the pipeline server
COLUMN_PROP_CONTENT_SENSITIVITY = "__sensitivity"

DEFAULT_NOT_SET = "DEFAULT_NOT_SET"

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

class AvroPrimitive(ABC, BaseModel):

    @abstractmethod
    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.null()

def get_class_name(v: Any) -> str:
    if isinstance(v, dict):
        t = v.get('type')
        l = v.get('logicalType')
        if l is not None:
            return l
        else:
            return t
    elif isinstance(v, list):
        return 'union'
    elif isinstance(v, str):
        return "native"
    elif isinstance(v, AvroUnion):
        return "union"
    else:
        t = getattr(v, 'type')
        if not hasattr(v, 'logicalType'):
            return t
        else:
            l = getattr(v, 'logicalType')
            if l is not None:
                return l
            else:
                return t

datatype_annotation = Annotated[
        (
            Annotated['RecordSchema', Tag('record')] |
            Annotated['ArraySchema', Tag('array')] |
            Annotated['AvroUnion', Tag('union')] |
            Annotated['AvroNative', Tag('native')] |
            Annotated['AvroBoolean', Tag('BOOLEAN')] |
            Annotated['AvroBoolean', Tag('boolean')] |
            Annotated['AvroByte', Tag('BYTE')] |
            Annotated['AvroBytes', Tag('BYTES')] |
            Annotated['AvroBytes', Tag('bytes')] |
            Annotated['AvroCLOB', Tag('CLOB')] |
            Annotated['AvroDate', Tag('date')] |
            Annotated['AvroDecimal', Tag('decimal')] |
            Annotated['AvroDouble', Tag('DOUBLE')] |
            Annotated['AvroDouble', Tag('double')] |
            Annotated['AvroEnum', Tag('ENUM')] |
            Annotated['AvroFixed', Tag('FIXED')] |
            Annotated['AvroFixed', Tag('fixed')] |
            Annotated['AvroFloat', Tag('FLOAT')] |
            Annotated['AvroFloat', Tag('float')] |
            Annotated['AvroInt', Tag('INT')] |
            Annotated['AvroInt', Tag('int')] |
            Annotated['AvroLocalTimestamp', Tag('local-timestamp-millis')] |
            Annotated['AvroLocalTimestampMicros', Tag('local-timestamp-micros')] |
            Annotated['AvroLong', Tag('LONG')] |
            Annotated['AvroLong', Tag('long')] |
            Annotated['AvroMap', Tag('MAP')] |
            Annotated['AvroMap', Tag('map')] |
            Annotated['AvroNCLOB', Tag('NCLOB')] |
            Annotated['AvroNull', Tag('NULL')] |
            Annotated['AvroNull', Tag('null')] |
            Annotated['AvroNVarchar', Tag('NVARCHAR')] |
            Annotated['AvroSTGeometry', Tag('ST_GEOMETRY')] |
            Annotated['AvroSTPoint', Tag('ST_POINT')] |
            Annotated['AvroShort', Tag('SHORT')] |
            Annotated['AvroString', Tag('STRING')] |
            Annotated['AvroString', Tag('string')] |
            Annotated['AvroTime', Tag('time-millis')] |
            Annotated['AvroTimeMicros', Tag('time-micros')] |
            Annotated['AvroTimestamp', Tag('timestamp-millis')] |
            Annotated['AvroTimestampMicros', Tag('timestamp-micros')] |
            Annotated['AvroUUID', Tag('uuid')] |
            Annotated['AvroUri', Tag('URI')] |
            Annotated['AvroVarchar', Tag('VARCHAR')]
        ),
        Discriminator(get_class_name),
    ]

datatype_union = Union[
            'RecordSchema',
            'ArraySchema',
            'AvroBoolean',
            'AvroByte',
            'AvroBytes',
            'AvroCLOB',
            'AvroDate',
            'AvroDecimal',
            'AvroDouble',
            'AvroEnum',
            'AvroFixed',
            'AvroFloat',
            'AvroFloat',
            'AvroInt',
            'AvroInt',
            'AvroLocalTimestamp',
            'AvroLocalTimestampMicros',
            'AvroLong',
            'AvroMap',
            'AvroMap',
            'AvroNCLOB',
            'AvroNull',
            'AvroNVarchar',
            'AvroSTGeometry',
            'AvroSTPoint',
            'AvroShort',
            'AvroString',
            'AvroTime',
            'AvroTimeMicros',
            'AvroTimestamp',
            'AvroTimestampMicros',
            'AvroUUID',
            'AvroUri',
            'AvroVarchar',
            'AvroNative'
    ]

class ColumnType(Enum):
    MEASURE="MEASURE"
    ATTRIBUTE="ATTRIBUTE"
    CURRENCY="CURRENCY"
    UOM="UOM"
    TEXT="TEXT"
    HIERARCHY="HIERARCHY"

class ColumnSemantic(BaseModel):
    type: Optional[ColumnType]
    aggregation_formula: Optional[str] = None
    """
    A SQL formula used to aggregate the values, e.g. sum(AMOUNT) or sum(BALANCE)/count(distinct BOOKING_DATE)
    """
    currency_field_name: Optional[str] = None
    """
    With field of the table contains the currency information for this amount column
    """
    currency_conversion_date: Optional[str] = None
    """
    The field name used to convert the currency, e.g. VALUTA_DATE
    """
    uom_field_name: Optional[str] = None
    """
    This field contains an unit of measure value, e.g. 10, and the field
    named "SALES_UNIT" is the corresponding UOM field with e.g. kg
    """
    hierarchy_name: Optional[str] = None
    """
    This field is part of an hierarchy with this name, e.g. the field COUNTRY, CITY both use the GEO hierarchy name
    """
    hierarchy_level: Optional[int] = None
    """
    This field is level n (starts with 1) of a hierarchy, e.g. COUNTRY is level 1, CITY level 2
    """

class Field(BaseModel):
    """
    A Field contains detailed metadata about the column, technical metadata and business metadata
    """
    name: str
    type: datatype_annotation
    nullable: bool = Pydantic_Field(default=False, exclude=True)
    doc: Optional[str] = None
    data_sensitivity: str = Pydantic_Field(alias=COLUMN_PROP_CONTENT_SENSITIVITY, default=DataSensitivityEnum.INTERNAL.name)
    source_data_type: Optional[str] = Pydantic_Field(alias=COLUMN_PROP_SOURCE_DATATYPE, default=None)
    default: Optional[Any] = Pydantic_Field(default=DEFAULT_NOT_SET, exclude_if=lambda v: v == DEFAULT_NOT_SET)
    original_name: Optional[str] = Pydantic_Field(alias=COLUMN_PROP_ORIGINAL_NAME, default=None)
    manual: bool = Pydantic_Field(default=False, exclude=True)
    internal: bool = Pydantic_Field(alias=COLUMN_PROP_INTERNAL, default=False)
    technical: bool = Pydantic_Field(alias=COLUMN_PROP_TECHNICAL, default=False)
    semantics: Optional[ColumnSemantic]

    model_config = ConfigDict(serialize_by_alias=True)

    def set_semantic_as_measure(self, formula: str):
        self.semantics = ColumnSemantic(type=ColumnType.MEASURE,
                                        aggregation_formula=formula)

    def set_sematic_as_attribute(self):
        self.semantics = ColumnSemantic(type=ColumnType.ATTRIBUTE)

    def set_semantic_as_currency(self, currency_field: str, currency_date_field: str):
        self.semantics = ColumnSemantic(type=ColumnType.CURRENCY,
                                        currency_field_name=currency_field,
                                        currency_conversion_date=currency_date_field)

    def set_semantic_as_text(self):
        self.semantics = ColumnSemantic(type=ColumnType.TEXT)

    def set_semantic_as_uom(self, uom_field: str):
        self.semantics = ColumnSemantic(type=ColumnType.UOM,
                                        uom_field_name=uom_field)

    def set_semantic_as_hierarchy(self, hierarchy_name: str, hierarchy_level: int):
        self.semantics = ColumnSemantic(type=ColumnType.HIERARCHY,
                                        hierarchy_name=hierarchy_name,
                                        hierarchy_level=hierarchy_level)

    @model_validator(mode='after')
    def set_values(self) -> Self:
        # a data type union[null, something] is unwrapped to data type something and the nullable flag set to true
        if not isinstance(self.type, AvroUnion) and not self.manual:
            # A complex union is never nullable
            self.nullable = False
        elif isinstance(self.type, AvroUnion) and isinstance(self.type.root[0], AvroNative) and self.type.root[0].root == 'null' and len(self.type.root) == 2:
            self.nullable = True
            self.type = self.type.root[1]
        if self.nullable:
            self.default = None
        self.original_name = decode_name(self.name) # name can be encoded or not encoded
        self.name = encode_name(self.original_name)
        return self

    @field_serializer('type', mode='plain')
    def serialize_nullable(self, value: Any) -> Any:
        if self.nullable:
            return ['null', value]
        else:
            return value

    def set_data_sensitivity(self, data_sensitivity: DataSensitivityEnum) -> "Field":
        self.data_sensitivity = data_sensitivity.name
        return self


class AvroNative(RootModel[str]):
    root: str

    def create_schema_dict(self) -> str:
        return self.root

    def get_pyarrow(self) -> any:
        match self.root:
            case 'string': return pyarrow.string()
            case 'null': return pyarrow.null()
            case 'boolean': return pyarrow.bool_()
            case 'int': return pyarrow.int32()
            case 'long': return pyarrow.int64()
            case 'float': return pyarrow.float32()
            case 'double': return pyarrow.float64()
            case 'bytes': return pyarrow.large_binary()
            case 'string': return pyarrow.string()


class AvroUnion(RootModel[list[datatype_union]]):
    root: list[datatype_union]

    def get_pyarrow(self) -> any:
        l = []
        for i in self.root:
            l.append(pyarrow.field(type(i).__name__, i.get_pyarrow()))
        return pyarrow.union(l, pyarrow.lib.UnionMode_DENSE)


class RecordSchema(BaseModel):

    type: Literal['record'] = 'record'
    name: str
    fields: list[Field] = list()
    namespace: Optional[str] = None
    doc: Optional[str] = None
    _field_name_index: dict[str, Field] = dict()
    _schema_name: Optional[str] = None
    original_name: Optional[str] = Pydantic_Field(alias=COLUMN_PROP_ORIGINAL_NAME, default=None)

    model_config = ConfigDict(serialize_by_alias=True)

    @model_serializer(mode='wrap')
    def serialize_model(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        serialized = handler(self)
        return serialized

    @model_validator(mode='after')
    def set_values(self) -> Self:
        self.original_name = decode_name(self.name) # input can be a plain or an encoded record name
        self.name = encode_name(self.original_name)
        if self.namespace is None:
            self._schema_name = self.original_name
        else:
            self._schema_name = self.namespace + "." + self.original_name
        for f in self.fields:
            self._field_name_index[f.name] = f
        return self

    def get_schema_name(self) -> str:
        """
        Return the full schema name, namespace.name
        """
        return self._schema_name

    def add_field(self, name: str, datatype: any, doc: Optional[str] = None, nullable: bool = True,
                  internal: bool = False, technical: bool = False, source_data_type: str = None, default: Any = DEFAULT_NOT_SET) -> Field:
        f = Field(name=name, type=datatype, nullable=nullable, doc=doc, internal=internal, technical=technical,
                  source_data_type=source_data_type, default=default, manual=True)
        # Don't know why the above internal and technical parameters are deemed missing, in case they have no default
        f.internal = internal
        f.technical = technical
        self.fields.append(f)
        self._field_name_index[name] = f
        return f

    def get_pyarrow(self) -> any:
        f = [(field.name, field.type.get_pyarrow()) for field in self.fields]
        return pyarrow.struct(f)

class ArraySchema(BaseModel):

    type: Literal['array'] = 'array'
    items: datatype_annotation

    def get_pyarrow(self) -> any:
        return pyarrow.list_(self.items.get_pyarrow())

class AvroAnyPrimitive(AvroPrimitive):
    union: AvroUnion

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
        self.union = AvroUnion(root=items)

    def get_pyarrow(self) -> pyarrow.DataType:
        return self.union.get_pyarrow()


class AvroBoolean(AvroPrimitive):
    type: Literal['boolean'] = 'boolean'
    logicalType: Optional[Literal['BOOLEAN']] = 'BOOLEAN'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.bool_()


class AvroByte(AvroPrimitive):
    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['BYTE']] = 'BYTE'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int8()


class AvroBytes(AvroPrimitive):

    type: Literal['bytes'] = 'bytes'
    logicalType: Optional[Literal['BYTES']] = 'BYTES'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.large_binary()


class AvroCLOB(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['CLOB']] = 'CLOB'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroDate(AvroPrimitive):

    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['date']] = 'date'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.date32()


class AvroDecimal(AvroPrimitive):

    type: Literal['bytes'] = 'bytes'
    logicalType: Optional[Literal['decimal']] = 'decimal'
    precision: int
    scale: int

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.decimal128(self.precision, self.scale)


class AvroDouble(AvroPrimitive):

    type: Literal['double'] = 'double'
    logicalType: Optional[Literal['DOUBLE']] = 'DOUBLE'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.float64()


class AvroEnum(AvroPrimitive):

    type: Literal['enum'] = 'enum'
    logicalType: Optional[Literal['ENUM']] = 'ENUM'
    name: str
    symbols: list[str]

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroFixed(AvroPrimitive):

    type: Literal['fixed'] = 'fixed'
    logicalType: Optional[Literal['FIXED']] = 'FIXED'
    size: int
    name: str

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.binary(self.length)


class AvroFloat(AvroPrimitive):

    type: Literal['float'] = 'float'
    logicalType: Optional[Literal['FLOAT']] = 'FLOAT'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.float32()


class AvroInt(AvroPrimitive):

    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['INT']] = 'INT'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int32()


class AvroLocalTimestamp(AvroPrimitive):

    type: Literal['long'] = 'long'
    logicalType: Optional[Literal['local-timestamp-millis']] = 'local-timestamp-millis'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("ms", tz=timezone.utc)


class AvroLocalTimestampMicros(AvroPrimitive):

    type: Literal['long'] = 'long'
    logicalType: Optional[Literal['local-timestamp-micros']] = 'local-timestamp-micros'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("us", tz=timezone.utc)


class AvroLong(AvroPrimitive):

    type: Literal['long'] = 'long'
    logicalType: Optional[Literal['LONG']] = 'LONG'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int64()


class AvroMap(AvroPrimitive):

    type: Literal['map'] = 'map'
    logicalType: Optional[Literal['MAP']] = 'MAP'
    values: datatype_annotation

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.map_(pyarrow.string(), self.value_data_type.get_pyarrow())


class AvroNCLOB(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['NCLOB']] = 'NCLOB'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroNVarchar(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['NVARCHAR']] = 'NVARCHAR'
    length: int

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroNull(AvroPrimitive):

    type: Literal['null'] = 'null'
    logicalType: Optional[Literal['NULL']] = 'NULL'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.null()


class AvroSTGeometry(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['ST_GEOMETRY']] = 'ST_GEOMETRY'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroSTPoint(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['ST_POINT']] = 'ST_POINT'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroShort(AvroPrimitive):

    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['SHORT']] = 'SHORT'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.int16()


class AvroString(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['STRING']] = 'STRING'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroTime(AvroPrimitive):

    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['time-millis']] = 'time-millis'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.time32("ms")


class AvroTimeMicros(AvroPrimitive):

    type: Literal['long'] = 'long'
    logicalType: Optional[Literal['time-micros']] = 'time-micros'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.time64("us")


class AvroTimestamp(AvroPrimitive):

    type: Literal['long'] = 'long'
    logicalType: Optional[Literal['timestamp-millis']] = 'timestamp-millis'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("ms", tz=timezone.utc)


class AvroTimestampMicros(AvroPrimitive):

    type: Literal['long'] = 'long'
    logicalType: Optional[Literal['timestamp-micros']] = 'timestamp-micros'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.timestamp("us", tz=timezone.utc)


class AvroUUID(AvroPrimitive):

    type: Literal['fixed'] = 'fixed'
    logicalType: Optional[Literal['uuid']] = 'uuid'
    size: int = 16

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.uuid()


class AvroUri(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['URI']] = 'URI'

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroVarchar(AvroPrimitive):

    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['VARCHAR']] = 'VARCHAR'
    length: int

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()

