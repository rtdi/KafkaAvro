from typing import Optional
import uuid

from kafkaavro.avro_datatypes import AvroString, AvroMap
from kafkaavro.schemabuilder import ValueSchema, RecordSchema, ArraySchema, KeySchema
from pydantic import BaseModel, Field

# Schema structure
#
# impact_lineage
# - producer_name
# - dataflow_name
# - target_tables
#   - target_table_name
#   - target_connection
#   - source_tables
#     - source_table_name
#     - source_connection
#     - mapping_formula
#     - mapping_description
#   - target_table_columnssource_tables
#     - column_name
#     - mapping_formula
#     - column_sources
#       - source_table_name
#       - source_connection
#       - source_column_name

# Example: Target table has a single column and its mapping is target1.col1 = source1.colA
# impact_lineage
# - producer_name: example_container
# - dataflow_name: None
# - target_tables
#   - target_table_name: target1
#   - target_connection: odbc://database1.company.com/
#   - source_tables
#     - source_table_name: source1
#     - source_connection: odbc://database2.company.com/
#     - mapping_formula: select col1 from source1
#     - mapping_description: Copy one column from the source
#   - target_table_columns
#     - column_name: col1
#     - mapping_formula: source1.colA
#     - column_sources
#       - source_table_name: source1
#       - source_connection: odbc://database2.company.com/
#       - source_column_name: colA

column_source_record = RecordSchema("column_source", None,
                                    "Table/column information of the source")
column_source_record.add_field("source_column_name", AvroString(),
                               "The source column name used in the mapping")
column_sources_map = AvroMap(ArraySchema(column_source_record))

# Above is contained in target_table_column
target_column_record = RecordSchema("target_table_column",
                                    None, "The column level mapping for each target column")
target_column_record.add_field("column_name", AvroString(),
                               "The column name of the target table", False)
target_column_record.add_field("mapping_formula", AvroString(),
                               "An approximation of the column mapping formula")
target_column_record.add_field("mapping_description", AvroString(),
                               "Optional free form text describing the column mapping")
target_column_record.add_field("column_sources", column_sources_map,
                               "All source table/columns impacting this target column")
target_columns_map = AvroMap(target_column_record)


source_table_record = RecordSchema("source_table", None,
                                   "Information about the source table")
source_table_record.add_field("source_table_name", AvroString(),
                              "The fully qualified source table name", False)
source_table_record.add_field("source_connection", AvroString(),
                              "The technical connection information of the target system, e.g. connection URL",
                              False)
source_table_record.add_field("mapping_formula", AvroString(),
                              "An approximation of the mapping, e.g. output=input or output=scd2(input)")
source_table_record.add_field("mapping_description", AvroString(),
                              "Optional free form text describing the mapping")
sources_map = AvroMap(source_table_record)

# source_table_record and target_column_record is contained in table_mapping
table_mapping_record = RecordSchema("target_table", None,
                                    "Contains the target table info and how it is loaded")
table_mapping_record.add_field("target_table_name", AvroString(),
                               "The fully qualified target table name", False)
table_mapping_record.add_field("target_connection", AvroString(),
                               "The technical connection information of the target system, e.g. connection URL",
                               False)
table_mapping_record.add_field("source_tables", sources_map,
                               "The list of source tables providing information for this target table")
table_mapping_record.add_field("target_columns", target_columns_map,
                                      "The list of target table columns")
table_mappings_map = AvroMap(table_mapping_record)




impact_lineage_value_schema = ValueSchema("impact_lineage", None)
impact_lineage_value_schema.set_pks({"producer_name", "dataflow_name"})
impact_lineage_value_schema.add_field("producer_name", AvroString(),
                                      "Producer name is the process name, e.g. a container "
                                      "name, a lambda name, a batch process", False)
impact_lineage_value_schema.add_field("dataflow_name", AvroString(),
                                      "Within a process name can be multiple sequential or parallel flows",
                                      True)
impact_lineage_value_schema.add_field("target_tables", table_mappings_map,
                                      "The list of source tables providing information for this target table")

impact_lineage_key_schema = KeySchema(impact_lineage_value_schema)

class SourceTable(BaseModel):

    source_table_name: str
    source_connection: str
    mapping_formula: str
    mapping_description: Optional[str] = None
    key: str = Field(default_factory=lambda: str(uuid.uuid4()), exclude=True)

    def get_key(self) -> str:
        return self.key


class TargetTable(BaseModel):

    source_tables: dict[str, SourceTable] = dict()
    target_table_name: str
    target_connection: str
    target_columns: dict[str, "TargetTableColumn"] = dict()

    def get_key(self) -> str:
        return self.target_table_name + "_" + self.target_connection

    def add_source_table(self, table: SourceTable) -> SourceTable:
        if table.get_key() not in self.source_tables:
            self.source_tables[table.get_key()] = table
            return table
        else:
            raise RuntimeError("Such a table exists already")

    def add_1_to_1_mapping(self, source: SourceTable, source_column_name: str, target_column_name: str,
                           mapping_description: str = "1:1"):
        self.target_columns[target_column_name] =\
            TargetTableColumn(column_name=target_column_name, mapping_formula="= " + source.source_table_name + "." + source_column_name,
                              mapping_description=mapping_description,
                              column_sources=dict())
        self.target_columns[target_column_name].add_column_source(source, source_column_name)

    def add_single_source_column_mapping(self, source: SourceTable, source_column_name: str, target_column_name: str,
                                         formula: str, mapping_description: str = "simple mapping"):
        self.target_columns[target_column_name] =\
            TargetTableColumn(column_name=target_column_name, mapping_formula=formula,
                              mapping_description=mapping_description,
                              column_sources=dict())
        self.target_columns[target_column_name].add_column_source(source, source_column_name)

    def add_constant_mapping(self, target_column_name: str,
                                         formula: str, mapping_description: str = "constant mapping"):
        self.target_columns[target_column_name] =\
            TargetTableColumn(column_name=target_column_name, mapping_formula=formula, mapping_description=mapping_description)

class ColumnSource(BaseModel):

    source_column_name: str

    def __hash__(self) -> int:
        return hash(self.source_column_name)


class TargetTableColumn(BaseModel):

    column_name: str
    mapping_formula: str
    mapping_description: Optional[str] = None
    column_sources: dict[str, list[ColumnSource]] = dict()

    def add_column_source(self, source_table: SourceTable, source_column_name: str):
        s: Optional[list[ColumnSource]] = self.column_sources.get(source_table.get_key())
        if s is None:
            s = list()
            self.column_sources[source_table.get_key()] = s
        s.append(ColumnSource(source_column_name=source_column_name))


class ImpactLineage(BaseModel):

    target_tables: dict[str, TargetTable] = dict()
    producer_name: str
    dataflow_name: str

    def add_target_table(self, target_table_name: str, target_connection: str) -> TargetTable:
        targets = self.target_tables
        target_table = TargetTable(target_table_name=target_table_name, target_connection=target_connection)
        if target_table.get_key() not in targets:
            targets[target_table.get_key()] = target_table
            return target_table
        else:
            raise RuntimeError("Table of that name exists already as target")

