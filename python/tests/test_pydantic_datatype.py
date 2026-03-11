from typing import Literal, Annotated, Any, Union, Optional
from pydantic import BaseModel, Tag, Discriminator, RootModel

class DatatypeSimple(RootModel):
    root: str

class DatatypeStr(BaseModel):
    type: Literal['string'] = 'string'
    logicalType: Optional[Literal['string']] = 'string'

class DatatypeInt(BaseModel):
    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['int']] = 'int'

class DatatypeDate(BaseModel):
    type: Literal['int'] = 'int'
    logicalType: Optional[Literal['date']] = 'date'

class DatatypeUnion(RootModel):
    root: list[Union[DatatypeStr, DatatypeInt, DatatypeSimple]]

def get_class_name(v: Any) -> str:
    if isinstance(v, dict):
        if v.get("logicalType") is not None:
            return v.get("logicalType")
        else:
            return v.get('type')
    elif isinstance(v, list):
        return 'union'
    elif isinstance(v, str):
        return "simple"
    else:
        if getattr(v, "logicalType") is not None:
            return getattr(v, "logicalType")
        else:
            return getattr(v, 'type')

datatype_annotation = Annotated[
        (
            Annotated['DatatypeStr', Tag('string')] |
            Annotated['DatatypeInt', Tag('int')] |
            Annotated['DatatypeSimple', Tag('simple')] |
            Annotated['DatatypeDate', Tag('date')] |
            Annotated['DatatypeUnion', Tag('union')]
        ),
        Discriminator(get_class_name),
    ]

class Field(BaseModel):
    type: datatype_annotation


if __name__ == '__main__':
    # This works
    print(Field.model_validate({'type': {'type': 'string', 'logicalType': 'string'}}))
    print(Field.model_validate({'type': {'type': 'int'}}))
    print(Field.model_validate({'type': {'type': 'int', 'logicalType': 'date'}}))
    print(Field.model_validate({'type': 'string'}))
    print(Field.model_validate({'type': 'int'}))
    # Unions do not
    print(Field.model_validate({'type': [ {'type': 'string'}, {'type': 'int'} ]}))
    print(Field.model_validate({'type': [ 'string', 'int']}))
