from datetime import timezone
from typing import Union

import pyarrow
from abc import ABC, abstractmethod


class AvroPrimitive(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def create_schema_dict(self) -> dict:
        pass

    @abstractmethod
    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.null()

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

    def __init__(self):
        super().__init__()

    def create_schema_dict(self) -> dict:
        return {"type": "enum", "logicalType": "ENUM"}

    def get_pyarrow(self) -> pyarrow.DataType:
        return pyarrow.string()


class AvroFixed(AvroPrimitive):

    def __init__(self, length: int):
        super().__init__()
        self.length = length

    def create_schema_dict(self) -> dict:
        return {"type": "fixed", "logicalType": "FIXED", "size": self.length}

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

    def __init__(self, value_data_type: AvroPrimitive):
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
