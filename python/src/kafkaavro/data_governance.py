from typing import List, Optional
from .table_constants import TimeUnit


class Duration:

    def __init__(self, value: int = None, unit: str = TimeUnit.YEARS.name):
        self.value: Optional[int] = value
        self.unit: str = unit


class DeletionPolicy:

    def __init__(self, value: Optional[int] = None, unit: str = TimeUnit.YEARS.name, description: Optional[str] = None):
        self.value: Optional[int] = value
        self.unit: str = unit
        self.description: Optional[str] = description


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


class JoinCondition:

    def __init__(self, left_field_name: str, right_field_name: str, condition: str = "="):
        self.left_field_name: str = left_field_name
        self.right_field_name: str = right_field_name
        self.condition: str = condition

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(left={self.left_field_name!r}, right={self.right_field_name!r}, condition={self.condition!r})"

