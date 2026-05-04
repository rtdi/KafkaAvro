from typing import List, Optional
from pydantic import BaseModel


class Duration(BaseModel):

    value: Optional[int]
    unit: str


class DeletionPolicy(BaseModel):

    value: Optional[int]
    unit: str
    description: str


class FKCondition(BaseModel):

    fk_name : str
    fk_schema_fqn: str
    conditions: List["JoinCondition"] = []

    def add_condition(self, left_field_name: str, right_field_name: str, condition: str = None):
        self.conditions.append(JoinCondition(left_field_name=left_field_name, right_field_name=right_field_name, condition=condition))


class JoinCondition(BaseModel):

    left_field_name: str
    right_field_name: str
    condition: str = "="


