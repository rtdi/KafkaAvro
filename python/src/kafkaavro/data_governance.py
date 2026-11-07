from typing import List, Optional
from .table_constants import TimeUnit


class Duration:

    def __init__(self, value: int = None, unit: str = TimeUnit.YEARS.name):
        self.value: Optional[int] = value
        self.unit: str = unit

    def create_schema_dict(self) -> dict:
        return {
            "value": self.value,
            "unit": self.unit
        }

def duration_from_dict(data: dict) -> Optional[Duration]:
    if data is not None:
        return Duration(value=data.get("value"), unit=data.get("unit"))

class DeletionPolicy:

    def __init__(self, value: Optional[int] = None, unit: str = TimeUnit.YEARS.name, description: Optional[str] = None):
        self.value: Optional[int] = value
        self.unit: str = unit
        self.description: Optional[str] = description

def deletion_policy_from_dict(data: dict) -> Optional[DeletionPolicy]:
    if data is not None:
        return DeletionPolicy(value=data.get("value"), unit=data.get("unit"), description=data.get("description"))
    else:
        return None

class FKCondition:

    def __init__(self, fk_schema_fqn: str, fk_name: Optional[str] = None,
                 left_field_name: str = None, right_field_name: str = None, condition: str = None):
        self.fk_name: str = fk_name
        self.fk_schema_fqn: Optional[str] = fk_schema_fqn
        self.conditions: List[JoinCondition] = []
        if left_field_name is not None or right_field_name is not None or condition is not None:
            self.add_condition(left_field_name, right_field_name, condition)

    def add_condition(self, left_field_name: str, right_field_name: str, condition: str = None):
        self.conditions.append(JoinCondition(left_field_name, right_field_name, condition))

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(fk_name={self.fk_name!r}, fk_schema_fql={self.fk_schema_fqn!r})"

    def create_schema_dict(self) -> dict:
        return {
            "fk_name": self.fk_name,
            "fk_schema_fqn": self.fk_schema_fqn,
            "conditions": [c.create_schema_dict() for c in self.conditions] if self.conditions else None
        }

class JoinCondition:

    def __init__(self, left_field_name: str, right_field_name: str, condition: str = "="):
        self.left_field_name: str = left_field_name
        self.right_field_name: str = right_field_name
        self.condition: str = condition

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(left={self.left_field_name!r}, right={self.right_field_name!r}, condition={self.condition!r})"

    def create_schema_dict(self) -> dict:
        return {
            "left_field_name": self.left_field_name,
            "right_field_name": self.right_field_name,
            "condition": self.condition
        }

def conditions_from_dict(data: list[dict]) -> Optional[list[JoinCondition]]:
    if data is not None:
        conditions: list[JoinCondition] = []
        for item in data:
            conditions.append(JoinCondition(left_field_name=item["left_field_name"],
                                            right_field_name=item["right_field_name"],
                                            condition=item["condition"]
                                            ))
        return conditions
    else:
        return None

def fk_from_dict(data: dict) -> Optional[FKCondition]:
    if data is not None:
        fk = FKCondition(fk_schema_fqn=data.get("fk_schema_fqn"),
                           fk_name=data.get("fk_name"))
        fk.conditions = conditions_from_dict(data.get("conditions"))
        return fk
    else:
        return None

def fks_from_dict(data: list[dict]) -> Optional[list[FKCondition]]:
    if data is not None:
        fks: list[FKCondition] = []
        for item in data:
            fks.append(fk_from_dict(item))
        return fks
    else:
        return None