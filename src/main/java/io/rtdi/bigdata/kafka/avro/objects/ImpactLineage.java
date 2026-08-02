package io.rtdi.bigdata.kafka.avro.objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;


public class ImpactLineage {

    public static ValueSchema impact_lineage_value_schema = new ValueSchema("impact_lineage", null);
    static {
        RecordSchema column_source_record = new RecordSchema("column_source", "Table/column information of the source");
        column_source_record.add("source_column_name", AvroString.create(),
                "The source column name used in the mapping", false);

        RecordSchema target_column_record = new RecordSchema("target_table_column", "The column level mapping for each target column");
        target_column_record.add("column_name", AvroString.create(),
                "The column name of the target table", false);
        target_column_record.add("mapping_formula", AvroString.create(),
                "An approximation of the column mapping formula", true);
        target_column_record.add("mapping_description", AvroString.create(),
                "Optional free form text describing the column mapping", true);
        target_column_record.add("column_sources", new AvroMap(column_source_record),
                "All source table/columns impacting this target column", true);

        RecordSchema source_table_record = new RecordSchema("source_table", "Information about the source table");
        source_table_record.add("source_table_name", AvroString.create(),
                "The fully qualified source table name", false);
        source_table_record.add("source_connection", AvroString.create(),
                "The technical connection information of the target system, e.g. connection URL",
                false);
        source_table_record.add("mapping_formula", AvroString.create(),
                "An approximation of the mapping, e.g. output=input or output=scd2(input)", true);
        source_table_record.add("mapping_description", AvroString.create(),
                "Optional free form text describing the mapping", true);
        
        RecordSchema table_mapping_record = new RecordSchema("target_table", "Contains the target table info and how it is loaded");
        table_mapping_record.add("target_table_name", AvroString.create(),
                "The fully qualified target table name", false);
        table_mapping_record.add("target_connection", AvroString.create(),
                "The technical connection information of the target system, e.g. connection URL",
                false);
        table_mapping_record.add("source_tables", new AvroMap(source_table_record),
                "The list of source tables providing information for this target table", true);
        table_mapping_record.add("target_columns", new AvroMap(target_column_record),
                "The list of target table columns", true);

        impact_lineage_value_schema.setPrimaryKeys("producer_name", "dataflow_name");
        impact_lineage_value_schema.add("producer_name", AvroString.create(),
                "Producer name is the process name, e.g. a container "
                        + "name, a lambda name, a batch process", false);
        impact_lineage_value_schema.add("dataflow_name", AvroString.create(),
                "Within a process name can be multiple sequential or parallel flows",
                true);
        impact_lineage_value_schema.add("target_tables", new AvroMap(table_mapping_record),
                "The list of source tables providing information for this target table", true);
    }


    private Map<String, TargetTable> targetTables = new HashMap<>();
    private String producerName;
    private String dataflowName;

    /**
     * Creates an empty impact-lineage instance.
     */
    public ImpactLineage() {
    }

    /**
     * Creates an impact-lineage entry for a producer and dataflow.
     *
     * @param producerName the producer process name
     * @param dataflowName the dataflow name
     */
    public ImpactLineage(String producerName, String dataflowName) {
        this.producerName = producerName;
        this.dataflowName = dataflowName;
    }

    public Map<String, TargetTable> getTargetTables() {
        return targetTables;
    }

    /**
     * Gets the producer name associated with the lineage entry.
     *
     * @return the producer name
     */
    public String getProducerName() {
        return producerName;
    }

    /**
     * Sets the producer name associated with the lineage entry.
     *
     * @param producerName the producer name
     */
    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    /**
     * Gets the dataflow name associated with the lineage entry.
     *
     * @return the dataflow name
     */
    public String getDataflowName() {
        return dataflowName;
    }

    /**
     * Sets the dataflow name associated with the lineage entry.
     *
     * @param dataflowName the dataflow name
     */
    public void setDataflowName(String dataflowName) {
        this.dataflowName = dataflowName;
    }

    /**
     * Replaces the schema used to describe impact-lineage records.
     *
     * @param impact_lineage_value_schema the schema object to use
     */
    public static void setImpact_lineage_value_schema(ValueSchema impact_lineage_value_schema) {
        ImpactLineage.impact_lineage_value_schema = impact_lineage_value_schema;
    }

    /**
     * Replaces the target-table mapping collection.
     *
     * @param targetTables the target-table map
     */
    public void setTargetTables(Map<String, TargetTable> targetTables) {
        this.targetTables = targetTables;
    }

    /**
     * Adds a target table to the lineage model.
     *
     * @param targetTableName the fully qualified target table name
     * @param targetConnection the target connection name
     * @return the created target-table mapping
     */
    public TargetTable addTargetTable(String targetTableName, String targetConnection) {
        TargetTable targetTable = new TargetTable(targetTableName, targetConnection);
        if (!targetTables.containsKey(targetTable.getKey())) {
            targetTables.put(targetTable.getKey(), targetTable);
            return targetTable;
        } else {
            throw new RuntimeException("Table of that name exists already as target");
        }
    }

    public static class SourceTable {
        private String sourceTableName;
        private String sourceConnection;
        private String mappingFormula;
        private String mappingDescription;
        private String key = UUID.randomUUID().toString();

        /**
         * Creates an empty source-table mapping.
         */
        public SourceTable() {
        }

        /**
         * Creates a source-table mapping with its connection and mapping metadata.
         *
         * @param sourceTableName the fully qualified source table name
         * @param sourceConnection the connection metadata for the source system
         * @param mappingFormula the mapping formula used for the source table
         * @param mappingDescription the descriptive text for the mapping
         */
        public SourceTable(String sourceTableName, String sourceConnection, String mappingFormula, String mappingDescription) {
            this.sourceTableName = sourceTableName;
            this.sourceConnection = sourceConnection;
            this.mappingFormula = mappingFormula;
            this.mappingDescription = mappingDescription;
        }

        /**
         * Gets the fully qualified source table name.
         *
         * @return the source table name
         */
        public String getSourceTableName() {
            return sourceTableName;
        }

        /**
         * Sets the fully qualified source table name.
         *
         * @param sourceTableName the source table name
         */
        public void setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
        }

        /**
         * Gets the connection metadata for the source system.
         *
         * @return the source connection information
         */
        public String getSourceConnection() {
            return sourceConnection;
        }

        /**
         * Sets the connection metadata for the source system.
         *
         * @param sourceConnection the source connection information
         */
        public void setSourceConnection(String sourceConnection) {
            this.sourceConnection = sourceConnection;
        }

        /**
         * Gets the mapping formula for the source table.
         *
         * @return the mapping formula
         */
        public String getMappingFormula() {
            return mappingFormula;
        }

        /**
         * Sets the mapping formula for the source table.
         *
         * @param mappingFormula the mapping formula
         */
        public void setMappingFormula(String mappingFormula) {
            this.mappingFormula = mappingFormula;
        }

        /**
         * Gets the descriptive text for the source-table mapping.
         *
         * @return the mapping description
         */
        public String getMappingDescription() {
            return mappingDescription;
        }

        /**
         * Sets the descriptive text for the source-table mapping.
         *
         * @param mappingDescription the mapping description
         */
        public void setMappingDescription(String mappingDescription) {
            this.mappingDescription = mappingDescription;
        }

        /**
         * Gets the unique key identifying the source-table mapping.
         *
         * @return the mapping key
         */
        public String getKey() {
            return key;
        }

        /**
         * Returns a hash code based on the source table name.
         *
         * @return the hash code for this mapping
         */
        @Override
        public int hashCode() {
            return Objects.hash(sourceTableName);
        }

        /**
         * Compares this source-table mapping to another object for equality.
         *
         * @param obj the object to compare against
         * @return {@code true} when the two mappings are equivalent
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            SourceTable that = (SourceTable) obj;
            return Objects.equals(sourceTableName, that.sourceTableName) &&
                Objects.equals(sourceConnection, that.sourceConnection) &&
                Objects.equals(mappingFormula, that.mappingFormula) &&
                Objects.equals(mappingDescription, that.mappingDescription);
        }

        /**
         * Sets the unique key for the source-table mapping.
         *
         * @param key the mapping key
         */
        public void setKey(String key) {
            this.key = key;
        }
    }

    public static class ColumnSource {
        private String sourceColumnName;

        /**
         * Creates an empty source-column mapping.
         */
        public ColumnSource() {
        }

        /**
         * Creates a source-column mapping with the source column name.
         *
         * @param sourceColumnName the source column name
         */
        public ColumnSource(String sourceColumnName) {
            this.sourceColumnName = sourceColumnName;
        }

        /**
         * Gets the source column name used in the mapping.
         *
         * @return the source column name
         */
        public String getSourceColumnName() {
            return sourceColumnName;
        }

        /**
         * Sets the source column name used in the mapping.
         *
         * @param sourceColumnName the source column name
         */
        public void setSourceColumnName(String sourceColumnName) {
            this.sourceColumnName = sourceColumnName;
        }

        /**
         * Returns a hash code based on the source column name.
         *
         * @return the hash code for this mapping
         */
        @Override
        public int hashCode() {
            return Objects.hash(sourceColumnName);
        }

        /**
         * Compares this source-column mapping to another object for equality.
         *
         * @param obj the object to compare against
         * @return {@code true} when the two mappings are equivalent
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ColumnSource that = (ColumnSource) obj;
            return Objects.equals(sourceColumnName, that.sourceColumnName);
        }

    }

    public static class TargetTableColumn {
        private String columnName;
        private String mappingFormula;
        private String mappingDescription;
        private Map<String, List<ColumnSource>> columnSources = new HashMap<>();

        /**
         * Creates an empty target-table column mapping.
         */
        public TargetTableColumn() {
        }

        /**
         * Creates a target-table column mapping with the column name and mapping metadata.
         *
         * @param columnName the target column name
         * @param mappingFormula the mapping formula for the column
         * @param mappingDescription the descriptive text for the mapping
         */
        public TargetTableColumn(String columnName, String mappingFormula, String mappingDescription) {
            this.columnName = columnName;
            this.mappingFormula = mappingFormula;
            this.mappingDescription = mappingDescription;
        }

        /**
         * Gets the target column name.
         *
         * @return the target column name
         */
        public String getColumnName() {
            return columnName;
        }

        /**
         * Sets the target column name.
         *
         * @param columnName the target column name
         */
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        /**
         * Gets the mapping formula for the target column.
         *
         * @return the mapping formula
         */
        public String getMappingFormula() {
            return mappingFormula;
        }

        /**
         * Sets the mapping formula for the target column.
         *
         * @param mappingFormula the mapping formula
         */
        public void setMappingFormula(String mappingFormula) {
            this.mappingFormula = mappingFormula;
        }

        /**
         * Gets the descriptive text for the target column mapping.
         *
         * @return the mapping description
         */
        public String getMappingDescription() {
            return mappingDescription;
        }

        /**
         * Sets the descriptive text for the target column mapping.
         *
         * @param mappingDescription the mapping description
         */
        public void setMappingDescription(String mappingDescription) {
            this.mappingDescription = mappingDescription;
        }

        public Map<String, List<ColumnSource>> getColumnSources() {
            return columnSources;
        }

        /**
         * Adds a source-column dependency to the target column mapping.
         *
         * @param sourceTable the source table that contributes the column
         * @param sourceColumnName the source column name
         */
        public void addColumnSource(SourceTable sourceTable, String sourceColumnName) {
            List<ColumnSource> list = columnSources.get(sourceTable.getKey());
            if (list == null) {
                list = new ArrayList<>();
                columnSources.put(sourceTable.getKey(), list);
            }
            list.add(new ColumnSource(sourceColumnName));
        }

        /**
         * Returns a hash code based on the target column name.
         *
         * @return the hash code for this mapping
         */
        @Override
        public int hashCode() {
            return Objects.hash(columnName);
        }

        /**
         * Compares this target-column mapping to another object for equality.
         *
         * @param obj the object to compare against
         * @return {@code true} when the two mappings are equivalent
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TargetTableColumn that = (TargetTableColumn) obj;
            return Objects.equals(columnName, that.columnName) &&
                Objects.equals(mappingFormula, that.mappingFormula) &&
                Objects.equals(mappingDescription, that.mappingDescription) &&
                Objects.equals(columnSources, that.columnSources);
        }

        /**
         * Replaces the source-column mapping collection for the target column.
         *
         * @param columnSources the source-column map
         */
        public void setColumnSources(Map<String, List<ColumnSource>> columnSources) {
            this.columnSources = columnSources;
        }

    }

    public static class TargetTable {
        private Map<String, SourceTable> sourceTables = new HashMap<>();
        private String targetTableName;
        private String targetConnection;
        private Map<String, TargetTableColumn> targetColumns = new HashMap<>();

        /**
         * Creates an empty target-table mapping.
         */
        public TargetTable() {
        }

        /**
         * Creates a target-table mapping with a table name and connection.
         *
         * @param targetTableName the fully qualified target table name
         * @param targetConnection the target connection information
         */
        public TargetTable(String targetTableName, String targetConnection) {
            this.targetTableName = targetTableName;
            this.targetConnection = targetConnection;
        }

        public Map<String, SourceTable> getSourceTables() {
            return sourceTables;
        }

        /**
         * Gets the fully qualified target table name.
         *
         * @return the target table name
         */
        public String getTargetTableName() {
            return targetTableName;
        }

        /**
         * Sets the fully qualified target table name.
         *
         * @param targetTableName the target table name
         */
        public void setTargetTableName(String targetTableName) {
            this.targetTableName = targetTableName;
        }

        /**
         * Gets the target connection information.
         *
         * @return the target connection information
         */
        public String getTargetConnection() {
            return targetConnection;
        }

        /**
         * Sets the target connection information.
         *
         * @param targetConnection the target connection information
         */
        public void setTargetConnection(String targetConnection) {
            this.targetConnection = targetConnection;
        }

        public Map<String, TargetTableColumn> getTargetColumns() {
            return targetColumns;
        }

        /**
         * Gets the unique key used to identify the target table mapping.
         *
         * @return the target table key
         */
        public String getKey() {
            return targetTableName + "_" + targetConnection;
        }

        /**
         * Replaces the source-table mapping collection.
         *
         * @param sourceTables the source-table map
         */
        public void setSourceTables(Map<String, SourceTable> sourceTables) {
            this.sourceTables = sourceTables;
        }

        /**
         * Replaces the target-column mapping collection.
         *
         * @param targetColumns the target-column map
         */
        public void setTargetColumns(Map<String, TargetTableColumn> targetColumns) {
            this.targetColumns = targetColumns;
        }

        /**
         * Adds a source-table mapping to the target table.
         *
         * @param table the source table mapping
         * @return the added source-table mapping
         */
        public SourceTable addSourceTable(SourceTable table) {
            if (!sourceTables.containsKey(table.getKey())) {
                sourceTables.put(table.getKey(), table);
                return table;
            } else {
                throw new RuntimeException("Such a table exists already");
            }
        }

        /**
         * Adds a one-to-one source-to-target column mapping.
         *
         * @param source the source table mapping
         * @param sourceColumnName the source column name
         * @param targetColumnName the target column name
         * @param mappingDescription the descriptive text for the mapping
         */
        public void addOneToOneMapping(SourceTable source, String sourceColumnName, String targetColumnName, String mappingDescription) {
            TargetTableColumn column = new TargetTableColumn(targetColumnName,
                    "= " + source.getSourceTableName() + "." + sourceColumnName,
                    mappingDescription);
            targetColumns.put(targetColumnName, column);
            column.addColumnSource(source, sourceColumnName);
        }

        /**
         * Adds a mapping that derives a target column from a single source column.
         *
         * @param source the source table mapping
         * @param sourceColumnName the source column name
         * @param targetColumnName the target column name
         * @param formula the mapping formula
         * @param mappingDescription the descriptive text for the mapping
         */
        public void addSingleSourceColumnMapping(SourceTable source, String sourceColumnName, String targetColumnName, String formula, String mappingDescription) {
            TargetTableColumn column = new TargetTableColumn(targetColumnName, formula, mappingDescription);
            targetColumns.put(targetColumnName, column);
            column.addColumnSource(source, sourceColumnName);
        }

        /**
         * Adds a constant-valued mapping for a target column.
         *
         * @param targetColumnName the target column name
         * @param formula the constant expression or formula
         * @param mappingDescription the descriptive text for the mapping
         */
        public void addConstantMapping(String targetColumnName, String formula, String mappingDescription) {
            TargetTableColumn column = new TargetTableColumn(targetColumnName, formula, mappingDescription);
            targetColumns.put(targetColumnName, column);
        }

        /**
         * Returns a hash code based on the target table name.
         *
         * @return the hash code for this target-table mapping
         */
        @Override
        public int hashCode() {
            return Objects.hash(targetTableName);
        }

        /**
         * Compares this target-table mapping to another object for equality.
         *
         * @param obj the object to compare against
         * @return {@code true} when the two mappings are equivalent
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TargetTable that = (TargetTable) obj;
            return Objects.equals(sourceTables, that.sourceTables) && 
                Objects.equals(targetTableName, that.targetTableName) && 
                Objects.equals(targetConnection, that.targetConnection) && 
                Objects.equals(targetColumns, that.targetColumns);
        }
    }
}
