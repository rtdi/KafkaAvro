from typing import List, Optional
from enum import Enum, auto

from pydantic import BaseModel


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


