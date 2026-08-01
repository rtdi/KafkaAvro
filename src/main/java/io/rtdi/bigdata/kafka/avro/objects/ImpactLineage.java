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
     * Creates a new instance of this class.
     */
    public ImpactLineage() {
    }

    /**
     * Creates a new instance of this class.
     * @param producerName the parameter value
     * @param dataflowName the parameter value
     */
    public ImpactLineage(String producerName, String dataflowName) {
        this.producerName = producerName;
        this.dataflowName = dataflowName;
    }

    public Map<String, TargetTable> getTargetTables() {
        return targetTables;
    }

    /**
     * Executes the String getProducerName operation.
     */
    public String getProducerName() {
        return producerName;
    }

    /**
     * Executes the void setProducerName operation.
     * @param producerName the parameter value
     */
    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    /**
     * Executes the String getDataflowName operation.
     */
    public String getDataflowName() {
        return dataflowName;
    }

    /**
     * Executes the void setDataflowName operation.
     * @param dataflowName the parameter value
     */
    public void setDataflowName(String dataflowName) {
        this.dataflowName = dataflowName;
    }

    /**
     * Executes the void setImpact_lineage_value_schema operation and returns the resulting value.
     * @param impact_lineage_value_schema the parameter value
     * @return the resulting value
     */
    public static void setImpact_lineage_value_schema(ValueSchema impact_lineage_value_schema) {
        ImpactLineage.impact_lineage_value_schema = impact_lineage_value_schema;
    }

    /**
     * Executes the void setTargetTables operation.
     * @param String the parameter value
     * @param targetTables the parameter value
     */
    public void setTargetTables(Map<String, TargetTable> targetTables) {
        this.targetTables = targetTables;
    }

    /**
     * Executes the TargetTable addTargetTable operation.
     * @param targetTableName the parameter value
     * @param targetConnection the parameter value
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
         * Executes the SourceTable operation.
         */
        public SourceTable() {
        }

        /**
         * Executes the SourceTable operation.
         * @param sourceTableName the parameter value
         * @param sourceConnection the parameter value
         * @param mappingFormula the parameter value
         * @param mappingDescription the parameter value
         */
        public SourceTable(String sourceTableName, String sourceConnection, String mappingFormula, String mappingDescription) {
            this.sourceTableName = sourceTableName;
            this.sourceConnection = sourceConnection;
            this.mappingFormula = mappingFormula;
            this.mappingDescription = mappingDescription;
        }

        /**
         * Executes the String getSourceTableName operation.
         */
        public String getSourceTableName() {
            return sourceTableName;
        }

        /**
         * Executes the void setSourceTableName operation.
         * @param sourceTableName the parameter value
         */
        public void setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
        }

        /**
         * Executes the String getSourceConnection operation.
         */
        public String getSourceConnection() {
            return sourceConnection;
        }

        /**
         * Executes the void setSourceConnection operation.
         * @param sourceConnection the parameter value
         */
        public void setSourceConnection(String sourceConnection) {
            this.sourceConnection = sourceConnection;
        }

        /**
         * Executes the String getMappingFormula operation.
         */
        public String getMappingFormula() {
            return mappingFormula;
        }

        /**
         * Executes the void setMappingFormula operation.
         * @param mappingFormula the parameter value
         */
        public void setMappingFormula(String mappingFormula) {
            this.mappingFormula = mappingFormula;
        }

        /**
         * Executes the String getMappingDescription operation.
         */
        public String getMappingDescription() {
            return mappingDescription;
        }

        /**
         * Executes the void setMappingDescription operation.
         * @param mappingDescription the parameter value
         */
        public void setMappingDescription(String mappingDescription) {
            this.mappingDescription = mappingDescription;
        }

        /**
         * Executes the String getKey operation.
         */
        public String getKey() {
            return key;
        }

        @Override
        /**
         * Executes the int hashCode operation.
         */
        public int hashCode() {
            return Objects.hash(sourceTableName);
        }

        @Override
        /**
         * Executes the boolean equals operation.
         * @param obj the parameter value
         */
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
         * Executes the void setKey operation.
         * @param key the parameter value
         */
        public void setKey(String key) {
            this.key = key;
        }
    }

    public static class ColumnSource {
        private String sourceColumnName;

        /**
         * Executes the ColumnSource operation.
         */
        public ColumnSource() {
        }

        /**
         * Executes the ColumnSource operation.
         * @param sourceColumnName the parameter value
         */
        public ColumnSource(String sourceColumnName) {
            this.sourceColumnName = sourceColumnName;
        }

        /**
         * Executes the String getSourceColumnName operation.
         */
        public String getSourceColumnName() {
            return sourceColumnName;
        }

        /**
         * Executes the void setSourceColumnName operation.
         * @param sourceColumnName the parameter value
         */
        public void setSourceColumnName(String sourceColumnName) {
            this.sourceColumnName = sourceColumnName;
        }

        @Override
        /**
         * Executes the int hashCode operation.
         */
        public int hashCode() {
            return Objects.hash(sourceColumnName);
        }

        @Override
        /**
         * Executes the boolean equals operation.
         * @param obj the parameter value
         */
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
         * Executes the TargetTableColumn operation.
         */
        public TargetTableColumn() {
        }

        /**
         * Executes the TargetTableColumn operation.
         * @param columnName the parameter value
         * @param mappingFormula the parameter value
         * @param mappingDescription the parameter value
         */
        public TargetTableColumn(String columnName, String mappingFormula, String mappingDescription) {
            this.columnName = columnName;
            this.mappingFormula = mappingFormula;
            this.mappingDescription = mappingDescription;
        }

        /**
         * Executes the String getColumnName operation.
         */
        public String getColumnName() {
            return columnName;
        }

        /**
         * Executes the void setColumnName operation.
         * @param columnName the parameter value
         */
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        /**
         * Executes the String getMappingFormula operation.
         */
        public String getMappingFormula() {
            return mappingFormula;
        }

        /**
         * Executes the void setMappingFormula operation.
         * @param mappingFormula the parameter value
         */
        public void setMappingFormula(String mappingFormula) {
            this.mappingFormula = mappingFormula;
        }

        /**
         * Executes the String getMappingDescription operation.
         */
        public String getMappingDescription() {
            return mappingDescription;
        }

        /**
         * Executes the void setMappingDescription operation.
         * @param mappingDescription the parameter value
         */
        public void setMappingDescription(String mappingDescription) {
            this.mappingDescription = mappingDescription;
        }

        public Map<String, List<ColumnSource>> getColumnSources() {
            return columnSources;
        }

        /**
         * Executes the void addColumnSource operation.
         * @param sourceTable the parameter value
         * @param sourceColumnName the parameter value
         */
        public void addColumnSource(SourceTable sourceTable, String sourceColumnName) {
            List<ColumnSource> list = columnSources.get(sourceTable.getKey());
            if (list == null) {
                list = new ArrayList<>();
                columnSources.put(sourceTable.getKey(), list);
            }
            list.add(new ColumnSource(sourceColumnName));
        }

        @Override
        /**
         * Executes the int hashCode operation.
         */
        public int hashCode() {
            return Objects.hash(columnName);
        }

        @Override
        /**
         * Executes the boolean equals operation.
         * @param obj the parameter value
         */
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
         * Executes the void setColumnSources operation.
         * @param String the parameter value
         * @param columnSources the parameter value
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
         * Executes the TargetTable operation.
         */
        public TargetTable() {
        }

        /**
         * Executes the TargetTable operation.
         * @param targetTableName the parameter value
         * @param targetConnection the parameter value
         */
        public TargetTable(String targetTableName, String targetConnection) {
            this.targetTableName = targetTableName;
            this.targetConnection = targetConnection;
        }

        public Map<String, SourceTable> getSourceTables() {
            return sourceTables;
        }

        /**
         * Executes the String getTargetTableName operation.
         */
        public String getTargetTableName() {
            return targetTableName;
        }

        /**
         * Executes the void setTargetTableName operation.
         * @param targetTableName the parameter value
         */
        public void setTargetTableName(String targetTableName) {
            this.targetTableName = targetTableName;
        }

        /**
         * Executes the String getTargetConnection operation.
         */
        public String getTargetConnection() {
            return targetConnection;
        }

        /**
         * Executes the void setTargetConnection operation.
         * @param targetConnection the parameter value
         */
        public void setTargetConnection(String targetConnection) {
            this.targetConnection = targetConnection;
        }

        public Map<String, TargetTableColumn> getTargetColumns() {
            return targetColumns;
        }

        /**
         * Executes the String getKey operation.
         */
        public String getKey() {
            return targetTableName + "_" + targetConnection;
        }

        /**
         * Executes the void setSourceTables operation.
         * @param String the parameter value
         * @param sourceTables the parameter value
         */
        public void setSourceTables(Map<String, SourceTable> sourceTables) {
            this.sourceTables = sourceTables;
        }

        /**
         * Executes the void setTargetColumns operation.
         * @param String the parameter value
         * @param targetColumns the parameter value
         */
        public void setTargetColumns(Map<String, TargetTableColumn> targetColumns) {
            this.targetColumns = targetColumns;
        }

        /**
         * Executes the SourceTable addSourceTable operation.
         * @param table the parameter value
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
         * Executes the void addOneToOneMapping operation.
         * @param source the parameter value
         * @param sourceColumnName the parameter value
         * @param targetColumnName the parameter value
         * @param mappingDescription the parameter value
         */
        public void addOneToOneMapping(SourceTable source, String sourceColumnName, String targetColumnName, String mappingDescription) {
            TargetTableColumn column = new TargetTableColumn(targetColumnName,
                    "= " + source.getSourceTableName() + "." + sourceColumnName,
                    mappingDescription);
            targetColumns.put(targetColumnName, column);
            column.addColumnSource(source, sourceColumnName);
        }

        /**
         * Executes the void addSingleSourceColumnMapping operation.
         * @param source the parameter value
         * @param sourceColumnName the parameter value
         * @param targetColumnName the parameter value
         * @param formula the parameter value
         * @param mappingDescription the parameter value
         */
        public void addSingleSourceColumnMapping(SourceTable source, String sourceColumnName, String targetColumnName, String formula, String mappingDescription) {
            TargetTableColumn column = new TargetTableColumn(targetColumnName, formula, mappingDescription);
            targetColumns.put(targetColumnName, column);
            column.addColumnSource(source, sourceColumnName);
        }

        /**
         * Executes the void addConstantMapping operation.
         * @param targetColumnName the parameter value
         * @param formula the parameter value
         * @param mappingDescription the parameter value
         */
        public void addConstantMapping(String targetColumnName, String formula, String mappingDescription) {
            TargetTableColumn column = new TargetTableColumn(targetColumnName, formula, mappingDescription);
            targetColumns.put(targetColumnName, column);
        }

        @Override
        /**
         * Executes the int hashCode operation.
         */
        public int hashCode() {
            return Objects.hash(targetTableName);
        }

        @Override
        /**
         * Executes the boolean equals operation.
         * @param obj the parameter value
         */
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
