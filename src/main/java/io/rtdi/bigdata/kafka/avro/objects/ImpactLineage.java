package io.rtdi.bigdata.kafka.avro.objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * ImpactLineage
 */
public class ImpactLineage {

    /**
     * The schema used to describe impact-lineage records.
     */
    public static final ValueSchema impact_lineage_value_schema = new ValueSchema("impact_lineage", null);
    public static final Schema avro_schema;
    private static final Schema avro_schema_targettable;
    private static final Schema avro_schema_sourcetable;
    private static final Schema avro_schema_columnsource;
    private static final Schema avro_schema_targettablecolumn;
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
        avro_schema = impact_lineage_value_schema.createSchema();
        avro_schema_targettable = AvroUtils.getBaseSchema(avro_schema.getField("target_tables").schema()).getValueType();
        avro_schema_sourcetable = AvroUtils.getBaseSchema(avro_schema_targettable.getField("source_tables").schema()).getValueType();
        avro_schema_targettablecolumn = AvroUtils.getBaseSchema(avro_schema_targettable.getField("target_columns").schema()).getValueType();
        avro_schema_columnsource = AvroUtils.getBaseSchema(avro_schema_targettablecolumn.getField("column_sources").schema()).getValueType();
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

    /**
     * Get the current object as GenericRecord
     * @return GenericRecord
     */
    public GenericData.Record toRecord() {
        GenericData.Record r = new GenericData.Record(avro_schema);
        r.put("target_tables", TargetTable.create(targetTables));
        r.put("producer_name", this.producerName);
        r.put("dataflow_name", this.dataflowName);
        return r;
    }

    /**
     * Create a ImpactLineage instance from the Avro record data
     * 
     * @param data Avro record
     * @return the ImpactLineage instance
     * @throws AvroTypeException in case the record does not match the structure
     */
    public static ImpactLineage from(GenericData.Record data) throws AvroTypeException {
        ImpactLineage t = new ImpactLineage();
        t.setDataflowName(AvroUtils.getAvroValue(data, "dataflow_name", String.class));
        t.setProducerName(AvroUtils.getAvroValue(data, "producer_name", String.class));
        t.setTargetTables(TargetTable.from(AvroUtils.getAvroMapOfRecords(data, "target_tables")));
        return t;
    }
    
	/**
	 * Deserializes a JSON payload into a {@link ImpactLineage} instance.
	 *
	 * @param json the JSON payload
	 * @return the parsed value schema
	 * @throws JsonMappingException if the JSON structure cannot be mapped
	 * @throws JsonProcessingException if the JSON payload cannot be read
	 */
	public static ImpactLineage fromRecordJson(String json) throws JsonMappingException, JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.readValue(json, ImpactLineage.class);
	}

    /**
     * Serialize the record values into Json
     * @return the Trigger values as string
     * @throws JsonProcessingException
     */
    public String toRecordJson() throws JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.writeValueAsString(this);
    }

    /**
     * Gets the target-table mapping collection.
     * @return the target-table map
     */
    @JsonProperty("target_tables")
    public Map<String, TargetTable> getTargetTables() {
        return targetTables;
    }

    /**
     * Gets the producer name associated with the lineage entry.
     *
     * @return the producer name
     */
    @JsonProperty("producer_name")
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
    @JsonProperty("dataflow_name")
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

    /**
     * Returns a hash code based on the parent function name.
     *
     * @return the hash code for this event set
     */
    @Override
    public int hashCode() { return Objects.hashCode(this.producerName); }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof ImpactLineage t) {
            return Objects.equals(this.producerName, t.producerName) && 
                Objects.equals(this.dataflowName, t.dataflowName) && 
                AvroUtils.isEqual(this.targetTables, t.targetTables);
        } else {
            return false;
        }
    }

    /**
     * A SourceTable represents a source table and has a unique, random key.
     */
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

        private static Map<String, SourceTable> from(Map<String, GenericData.Record> data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                Map<String, SourceTable> m = new HashMap<>();
                for(Entry<String, GenericData.Record> e : data.entrySet()) {
                    m.put(e.getKey(), SourceTable.from(e.getValue()));
                }
                return m;
            }
        }

        private static SourceTable from(GenericData.Record data) {
            SourceTable t = new SourceTable();
            t.setSourceTableName(AvroUtils.getAvroValue(data, "source_table_name", String.class));
            t.setSourceConnection(AvroUtils.getAvroValue(data, "source_connection", String.class));
            t.setMappingFormula(AvroUtils.getAvroValue(data, "mapping_formula", String.class));
            t.setMappingDescription(AvroUtils.getAvroValue(data, "mapping_description", String.class));
            return t;
        }

        private static Map<String, GenericData.Record> create(Map<String, SourceTable> sourcetables) {
            if (sourcetables == null) {
                return null;
            } else {
                Map<String, GenericData.Record> records = new HashMap<>();
                for (Entry<String, SourceTable> e : sourcetables.entrySet()) {
                    records.put(e.getKey(), e.getValue().create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_sourcetable);
            r.put("source_table_name", this.sourceTableName);
            r.put("source_connection", this.sourceConnection);
            r.put("mapping_formula", this.mappingFormula);
            r.put("mapping_description", this.mappingDescription);
            return r;
        }

        /**
         * Gets the fully qualified source table name.
         *
         * @return the source table name
         */
        @JsonProperty("source_table_name")
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
        @JsonProperty("source_connection")
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
        @JsonProperty("mapping_formula")
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
        @JsonProperty("mapping_description")
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

    /**
     * A ColumnSource represents a source table column.
     */
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

        @SuppressWarnings("rawtypes")
        private static Map<String, List<ColumnSource>> from(Map<String, List> data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                Map<String, List<ColumnSource>> m = new HashMap<>();
                for(Entry<String, List> e : data.entrySet()) {
                    m.put(e.getKey(), ColumnSource.from(e.getValue()));
                }
                return m;
            }
        }

        @SuppressWarnings("rawtypes")
        private static List<ColumnSource> from(List data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                List<ColumnSource> m = new ArrayList<>();
                for(Object e : data) {
                    if (e instanceof GenericData.Record r) {
                        m.add(ColumnSource.from(r));
                    } else {

                    }
                }
                return m;
            }
        }

        private static ColumnSource from(GenericData.Record data) {
            ColumnSource t = new ColumnSource();
            t.setSourceColumnName(AvroUtils.getAvroValue(data, "source_column_name", String.class));
            return t;
        }

        private static Map<String, List<GenericData.Record>> create(Map<String, List<ColumnSource>> columns) {
            if (columns == null) {
                return null;
            } else {
                Map<String, List<GenericData.Record>> records = new HashMap<>();
                for (Entry<String, List<ColumnSource>> e : columns.entrySet()) {
                    if (e != null) {
                        List<GenericData.Record> l = new ArrayList<>();
                        for( ColumnSource r : e.getValue()) {
                            l.add(r.create());
                        }
                        records.put(e.getKey(), l);
                    }
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_columnsource);
            r.put("source_column_name", this.sourceColumnName);
            return r;
        }

        /**
         * Gets the source column name used in the mapping.
         *
         * @return the source column name
         */
        @JsonProperty("source_column_name")
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

    /**
     * A TargetTableColumn represents a target table column and contains the column level mapping.
     */
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

        private static Map<String, TargetTableColumn> from(Map<String, GenericData.Record> data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                Map<String, TargetTableColumn> m = new HashMap<>();
                for(Entry<String, GenericData.Record> e : data.entrySet()) {
                    m.put(e.getKey(), TargetTableColumn.from(e.getValue()));
                }
                return m;
            }
        }

        private static TargetTableColumn from(GenericData.Record data) {
            TargetTableColumn t = new TargetTableColumn();
            t.setColumnName(AvroUtils.getAvroValue(data, "column_name", String.class));
            t.setMappingFormula(AvroUtils.getAvroValue(data, "mapping_formula", String.class));
            t.setMappingDescription(AvroUtils.getAvroValue(data, "mapping_description", String.class));
            t.setColumnSources(ColumnSource.from(AvroUtils.getAvroMap(data, "column_sources", List.class)));
            return t;
        }

        private static Map<String, GenericData.Record> create(Map<String, TargetTableColumn> columns) {
            if (columns == null) {
                return null;
            } else {
                Map<String, GenericData.Record> records = new HashMap<>();
                for (Entry<String, TargetTableColumn> e : columns.entrySet()) {
                    records.put(e.getKey(), e.getValue().create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_targettablecolumn);
            r.put("column_name", this.columnName);
            r.put("mapping_formula", this.mappingFormula);
            r.put("mapping_description", this.mappingDescription);
            r.put("column_sources", ColumnSource.create(this.columnSources));
            return r;
        }

        /**
         * Gets the target column name.
         *
         * @return the target column name
         */
        @JsonProperty("column_name")
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
        @JsonProperty("mapping_formula")
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
        @JsonProperty("mapping_description")
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

        /**
         * Gets the source-column mapping collection for the target column.
         * @return the source-column map
         */
        @JsonProperty("column_sources")
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

    /**
     * A TargetTable holds all information about which source tables contribute to it and the column level mapping for each target column.
     */
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

        private static Map<String, TargetTable> from(Map<String, GenericData.Record> data) throws AvroTypeException {
            if (data == null) {
                return null;
            } else {
                Map<String, TargetTable> m = new HashMap<>();
                for(Entry<String, GenericData.Record> e : data.entrySet()) {
                    m.put(e.getKey(), TargetTable.from(e.getValue()));
                }
                return m;
            }
        }

        private static TargetTable from(GenericData.Record data) {
            TargetTable t = new TargetTable();
            t.setTargetTableName(AvroUtils.getAvroValue(data, "target_table_name", String.class));
            t.setTargetConnection(AvroUtils.getAvroValue(data, "target_connection", String.class));
            t.setSourceTables(SourceTable.from(AvroUtils.getAvroMapOfRecords(data, "source_tables")));
            t.setTargetColumns(TargetTableColumn.from(AvroUtils.getAvroMapOfRecords(data, "target_columns")));
            return t;
        }

        private static Map<String, GenericData.Record> create(Map<String, TargetTable> targettables) {
            if (targettables == null) {
                return null;
            } else {
                Map<String, GenericData.Record> records = new HashMap<>();
                for (Entry<String, TargetTable> e : targettables.entrySet()) {
                    records.put(e.getKey(), e.getValue().create());
                }
                return records;
            }
        }

        private GenericData.Record create() {
            GenericData.Record r = new GenericData.Record(avro_schema_targettable);
            r.put("source_tables", SourceTable.create(this.sourceTables));
            r.put("target_table_name", this.targetTableName);
            r.put("target_connection", this.targetConnection);
            r.put("target_columns", TargetTableColumn.create(this.targetColumns));
            return r;
        }

        /**
         * Gets the source-table mapping collection for the target table.
         * @return the source-table map
         */
        @JsonProperty("source_tables")
        public Map<String, SourceTable> getSourceTables() {
            return sourceTables;
        }

        /**
         * Gets the fully qualified target table name.
         *
         * @return the target table name
         */
        @JsonProperty("target_table_name")
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
        @JsonProperty("target_connection")
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

        /**
         * Adds a source-table mapping to the target table.
         * @return the added source-table mapping
         */
        @JsonProperty("target_columns")
        public Map<String, TargetTableColumn> getTargetColumns() {
            return targetColumns;
        }

        /**
         * Gets the unique key used to identify the target table mapping.
         *
         * @return the target table key
         */
        @JsonIgnore
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
            return AvroUtils.isEqual(sourceTables, that.sourceTables) && 
                Objects.equals(targetTableName, that.targetTableName) && 
                Objects.equals(targetConnection, that.targetConnection) && 
                AvroUtils.isEqual(targetColumns, that.targetColumns);
        }
    }
}
