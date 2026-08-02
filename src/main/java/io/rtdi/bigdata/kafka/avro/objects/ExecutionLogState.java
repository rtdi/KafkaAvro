package io.rtdi.bigdata.kafka.avro.objects;

/**
 * The state of the execution log for a dataflow.
 */
public enum ExecutionLogState {
    /**
     * The execution has started.
     */
    STARTED,
    /**
     * The execution has completed.
     */
    COMPLETED,
    /**
     * The execution is waiting for retry
     */
    RETRY,
    /**
     * The execution has failed.
     */
    FAILED
}