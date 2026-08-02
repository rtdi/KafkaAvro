package io.rtdi.bigdata.kafka.avro.objects;

/**
 * Can be either an initial load or a delta load.
 */
public enum LoadType {
    /** An initial load. */
    INITIAL,
    /** A delta load. */
    DELTA
}