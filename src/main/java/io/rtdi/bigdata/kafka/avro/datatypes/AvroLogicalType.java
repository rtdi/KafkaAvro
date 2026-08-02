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
     * Get the type of the logical type.
     * @return the resulting value
     */
    public String getType() {
        return getName();
    }

    /**
     * Set the type of the logical type.
     * @param name the parameter value
     */
    public void setType(String name) {
        // ignore, this is only for Jackson to be able to deserialize the type
    }
}
