from enum import Enum, auto

ROW_SOURCE_SYSTEM = "__source_system"
ROW_SOURCE_TRANSACTION = "__source_transaction"
ROW_RECORD_ID = "__source_rowid"
ROW_CHANGE_TS = "__change_time"
ROW_TYPE_FIELD = "__change_type"
ROW_TRUNCATE = "__truncate"
SCHEMA_COLUMN_EXTENSION = "__extension"
SCHEMA_INFO_TICKETS_URL = "tickets_url"
SCHEMA_INFO_REPO_URL = "repo_url"
SCHEMA_INFO_DATAPRODUCT_OWNER = "data_product_owner_email"
SOURCE_SYSTEM_URI = 'source_system_uri'
DATA_CLASSIFICATIONS = 'data_classifications'
DELETION_POLICY = 'deletion_policy'
RETENTION_PERIOD = 'retention_period'
AUDIT = "__audit"
FKS = "fks"
PKS = "pks"
OBJECT_LEVEL_SECURITY = "object_level_security"
ROW_LEVEL_SECURITY = "row_level_security"
PARTITION_BY = "partition_by"
SEMANTICS = "semantics"

COLUMN_PROP_SOURCE_DATATYPE = "__source_data_type"
COLUMN_PROP_ORIGINAL_NAME = "__originalname"
COLUMN_PROP_INTERNAL = "__internal"
COLUMN_PROP_TECHNICAL = "__technical" # cannot be used in mappings as the values are set when sending the rows to the pipeline server
COLUMN_PROP_CONTENT_SENSITIVITY = "__sensitivity"
DEFAULT_NOT_SET = "DEFAULT_NOT_SET"


class TableType(Enum):
    FACT="FACT"
    """
    This is primarily a fact table
    """
    DIMENSION="DIMENSION"
    """
    This is primarily a dimension table
    """
    FACT_DIMENSION="FACT_DIMENSION"
    """
    This table has the character of a dimension and a fact, e.g. the sales order header is a fact
    but also a dimension for the sales order line item table.
    """
    MULTILANGUAGE_TEXT="MULTILANGUAGE_TEXT"
    """
    A table with ID and LANGUAGE as logical primary key and a test field
    """
    PARENT_CHILD_HIERARCHY="PARENT_CHILD_HIERARCHY"
    """
    Contains a hierarchy in form of a parent child table
    """
    CURRENCY_CONVERSION="CURRENCY_CONVERSION"
    """
    A table used to convert one currency into another
    """
    UOM_CONVERSION="UOM_CONVERSION"
    """
    A table used to convert one unit of measure into another, e.g. kg into tons
    """
    BRIDGE_TABLE="BRIDGE_TABLE"
    """
    A technical table used to resolve m:n relationships
    """
    VALUE_MAPPING="VALUE_MAPPING"
    """
    Maps a value of one system to another system, e.g. a SAP customer number to a Salesforce customer id
    """
    INTERNAL="INTERNAL"
    """
    Mark this table as internal - not meant to be used by anybody but data engineers
    """
    SECURITY="SECURITY"
    """
    This table contains permission related information, e.g. users, roles, role assignments, row level security filters
    """


class ColumnType(Enum):
    MEASURE="MEASURE"
    ATTRIBUTE="ATTRIBUTE"
    CURRENCY="CURRENCY"
    UOM="UOM"
    TEXT="TEXT"
    HIERARCHY="HIERARCHY"


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

