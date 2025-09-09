from typing import List, Optional
from enum import Enum, auto


class DataSensitivityEnum(str, Enum):
    INTERNAL = "INTERNAL"
    GBU_ONLY = "GBU_ONLY"
    ITAR = "ITAR"
    DUAL_USE = "DUAL_USE"

    # This data can be shown to everybody logged in
    PUBLIC = "PUBLIC"
    # This data should not be shown to everybody logged in. The data itself is not the problem
    # but in combination with other data personal information might be derived.
    SENSITIVE = "SENSITIVE"
    # This data by itself is highly sensitive, it is personal information.
    # Examples like Social Security Number, Credit Card information, address details.
    PRIVATE = "PRIVATE"
    # Data falling under certain regulations like EAR, Dual Use, ITAR and others
    REGULATED = "REGULATED"
    # Personally Identifiable Information
    PII = "PII"
    # Protected Health Information
    PHI = "PHI"


class TimeUnit(str, Enum):
    HOURS = auto()
    WEEKS = auto()
    MONTHS = auto()
    QUARTERS = auto()
    YEARS = auto()


class Duration:

    def __init__(self, value: int = None, unit: TimeUnit = TimeUnit.YEARS):
        self.value = value  # type: Optional[int]
        self.unit = unit  # type: TimeUnit


class DeletionPolicy:

    def __init__(self, value: int = None, unit: TimeUnit = TimeUnit.YEARS, description: str = None):
        self.value = value  # type: Optional[int]
        self.unit = unit  # type: TimeUnit
        self.description = description


class FKCondition:

    def __init__(self, fk_schema_fqn: str, fk_name: Optional[str] = None,
                 left_field_name: str = None, right_field_name: str = None, condition: str = None):
        self.fk_name = fk_name
        self.fk_schema_fqn = fk_schema_fqn
        self.conditions = []  # type: List[JoinCondition]
        if left_field_name is not None or right_field_name is not None or condition is not None:
            self.add_condition(left_field_name, right_field_name, condition)

    def add_condition(self, left_field_name: str, right_field_name: str, condition: str = None):
        self.conditions.append(JoinCondition(left_field_name, right_field_name, condition))

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(fk_name={self.fk_name!r}, fk_schema_fql={self.fk_schema_fqn!r})"


class JoinCondition:

    def __init__(self, left_field_name: str, right_field_name: str, condition: str = "="):
        self.left_field_name = left_field_name
        self.right_field_name = right_field_name
        self.condition = condition

    def __repr__(self):
        class_name = type(self).__name__
        return f"{class_name}(left={self.left_field_name!r}, right={self.right_field_name!r}, condition={self.condition!r})"

