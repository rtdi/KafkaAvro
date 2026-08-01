package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.LogicalType;

public class AvroLogicalType extends LogicalType {
    /**
     * Creates a new instance of this class.
     * @param name the parameter value
     */
    public AvroLogicalType(String name) {
        super(name);
    }

    /**
     * Executes the String getType operation.
     */
    public String getType() {
        return getName();
    }

    /**
     * Executes the void setType operation.
     * @param name the parameter value
     */
    public void setType(String name) {
        // ignore, this is only for Jackson to be able to deserialize the type
    }
}
