from typing import Optional

from .avro_datatypes import AvroString
from .schemabuilder import ValueSchema, RecordSchema, ArraySchema, get_key_schema

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
#   - target_table_columns
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
column_source_record.add_field("source_table_name", AvroString(),
                               "The fully qualified source table name")
column_source_record.add_field("source_connection", AvroString(),
                               "The technical connection information of the target system, e.g. connection URL")
column_source_record.add_field("source_column_name", AvroString(),
                               "The source column name used in the mapping")
column_sources_array = ArraySchema(column_source_record)

# Above is contained in target_table_column
target_column_record = RecordSchema("target_table_column",
                                    None, "The column level mapping for each target column")
target_column_record.add_field("column_name", AvroString(),
                               "The column name of the target table", False)
target_column_record.add_field("mapping_formula", AvroString(),
                               "An approximation of the column mapping formula")
target_column_record.add_field("mapping_description", AvroString(),
                               "Optional free form text describing the column mapping")
target_column_record.add_field("column_sources", column_sources_array,
                               "All source table/columns impacting this target column")
target_columns_array = ArraySchema(target_column_record)


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
sources_array = ArraySchema(source_table_record)

# source_table_record and target_column_record is contained in table_mapping
table_mapping_record = RecordSchema("target_table", None,
                                    "Contains the target table info and how it is loaded")
table_mapping_record.add_field("target_table_name", AvroString(),
                               "The fully qualified target table name", False)
table_mapping_record.add_field("target_connection", AvroString(),
                               "The technical connection information of the target system, e.g. connection URL",
                               False)
table_mapping_record.add_field("source_tables", sources_array,
                               "The list of source tables providing information for this target table")
table_mapping_record.add_field("target_columns", target_columns_array,
                                      "The list of target table columns")
table_mappings_array = ArraySchema(table_mapping_record)




impact_lineage_value_schema = ValueSchema("impact_lineage", None)
impact_lineage_value_schema.set_pks({"producer_name", "dataflow_name"})
impact_lineage_value_schema.add_field("producer_name", AvroString(),
                                      "Producer name is the process name, e.g. a container "
                                      "name, a lambda name, a batch process", False)
impact_lineage_value_schema.add_field("dataflow_name", AvroString(),
                                      "Within a process name can be multiple sequential or parallel flows",
                                      True)
impact_lineage_value_schema.add_field("target_tables", table_mappings_array,
                                      "The list of source tables providing information for this target table")

impact_lineage_key_schema = get_key_schema(impact_lineage_value_schema)

class SourceTable:

    def __init__(self, source_table_name: str, source_connection: str, mapping_formula: str,
                 mapping_description: Optional[str] = None):
        self.source_table_name = source_table_name
        self.source_connection = source_connection
        self.mapping_formula = mapping_formula
        self.mapping_description = mapping_description
        self.key = source_table_name + "_" + source_connection

    def create_dict(self):
        return {
            "source_table_name": self.source_table_name,
            "source_connection": self.source_connection,
            "mapping_formula": self.mapping_formula,
            "mapping_description": self.mapping_description
        }


class TargetTable:

    def __init__(self, target_table_name: str, target_connection: str):
        source_tables: dict[str, SourceTable] = dict()
        self.source_tables = source_tables
        self.target_table_name = target_table_name
        self.target_connection = target_connection
        target_table_columns: dict[str, TargetTableColumn] = dict()
        self.target_table_columns = target_table_columns
        self.key = target_table_name + "_" + target_connection

    def add_source_table(self, table: SourceTable):
        if table.key not in self.source_tables:
            self.source_tables[table.key] = table
        else:
            raise RuntimeError("Such a table exists already")

    def add_1_to_1_mapping(self, source: SourceTable, source_column_name: str, target_column_name: str,
                           mapping_description: str = "1:1"):
        self.target_table_columns[target_column_name] =\
            TargetTableColumn(target_column_name, "= " + source.source_table_name + "." + source_column_name,
                              mapping_description,
                              [ColumnSource(source, source_column_name)])

    def add_single_source_column_mapping(self, source: SourceTable, source_column_name: str, target_column_name: str,
                                         formula: str, mapping_description: str = "simple mapping"):
        self.target_table_columns[target_column_name] =\
            TargetTableColumn(target_column_name, formula,
                              mapping_description,
                              [ColumnSource(source, source_column_name)])

    def add_constant_mapping(self, source: SourceTable, target_column_name: str,
                                         formula: str, mapping_description: str = "constant mapping"):
        self.target_table_columns[target_column_name] =\
            TargetTableColumn(target_column_name, formula, mapping_description)

    def create_dict(self):
        return {
            "target_table_name": self.target_table_name,
            "target_connection": self.target_connection,
            "source_tables": [t.create_dict() for t in self.source_tables.values()],
            "target_columns": [t.create_dict() for t in self.target_table_columns.values()]
        }

class ColumnSource:

    def __init__(self, source_table: SourceTable, source_column_name: str):
        self.source_column_name = source_column_name
        self.source_table_name = source_table.source_table_name
        self.source_connection = source_table.source_connection

    def create_dict(self):
        return {
            "source_column_name": self.source_column_name,
            "source_table_name": self.source_table_name,
            "source_connection": self.source_connection
        }


class TargetTableColumn:

    def __init__(self, column_name: str, mapping_formula: str, mapping_description: str = None,
                 column_sources: list[ColumnSource] = None):
        self.column_name = column_name
        self.mapping_formula = mapping_formula
        self.mapping_description = mapping_description
        self.column_sources = column_sources
        if self.column_sources is None:
            self.column_sources = list()

    def add_column_source(self, source_table: SourceTable, source_column_name: str):
        self.column_sources.append(ColumnSource(source_table, source_column_name))

    def create_dict(self):
        return {
            "column_name": self.column_name,
            "mapping_formula": self.mapping_formula,
            "mapping_description": self.mapping_description,
            "column_sources": [t.create_dict() for t in self.column_sources]
        }


class ImpactLineage:

    def __init__(self, producer_name: str, dataflow_name: str):
        target_tables: dict[str, TargetTable] = dict()
        self.producer_name = producer_name
        self.dataflow_name = dataflow_name
        self.target_tables = target_tables

    def add_target_table(self, target_table_name: str, target_connection: str) -> TargetTable:
        targets = self.target_tables
        target_table = TargetTable(target_table_name, target_connection)
        if target_table.key not in targets:
            targets[target_table.key] = target_table
            return target_table
        else:
            raise RuntimeError("Table of that name exists already as target")

    def create_dict(self):
        return {
            "producer_name": self.producer_name,
            "dataflow_name": self.dataflow_name,
            "target_tables": [t.create_dict() for t in self.target_tables.values()]
        }
