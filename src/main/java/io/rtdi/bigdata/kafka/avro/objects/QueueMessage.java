package io.rtdi.bigdata.kafka.avro.objects;

/**
 * Represents a message in the processing queue, used to invoke dataflows.
 */
public class QueueMessage {
    private String id;
    private String loadType;
    private String dataflowName;
    private Integer partition;
    private String commitId;
    private String deltaPointer;
    private String calledBy;
    private String callerReason;

    /**
     * Creates an empty queue message instance.
     */
    public QueueMessage() {}

    /**
     * Gets the message identifier.
     *
     * @return the message id
     */
    public String getId() { return id; }
    /**
     * Sets the message identifier.
     *
     * @param id the message id
     */
    public void setId(String id) { this.id = id; }
    /**
     * Gets the load type for the queue message.
     *
     * @return the load type
     */
    public String getLoadType() { return loadType; }
    /**
     * Sets the load type for the queue message.
     *
     * @param loadType the load type
     */
    public void setLoadType(String loadType) { this.loadType = loadType; }
    /**
     * Gets the dataflow name associated with the queue message.
     *
     * @return the dataflow name
     */
    public String getDataflowName() { return dataflowName; }
    /**
     * Sets the dataflow name associated with the queue message.
     *
     * @param dataflowName the dataflow name
     */
    public void setDataflowName(String dataflowName) { this.dataflowName = dataflowName; }
    /**
     * Gets the partition number for the queue message.
     *
     * @return the partition number
     */
    public Integer getPartition() { return partition; }
    /**
     * Sets the partition number for the queue message.
     *
     * @param partition the partition number
     */
    public void setPartition(Integer partition) { this.partition = partition; }
    /**
     * Gets the commit identifier associated with the queue message.
     *
     * @return the commit id
     */
    public String getCommitId() { return commitId; }
    /**
     * Sets the commit identifier associated with the queue message.
     *
     * @param commitId the commit id
     */
    public void setCommitId(String commitId) { this.commitId = commitId; }
    /**
     * Gets the delta pointer for the queue message.
     *
     * @return the delta pointer
     */
    public String getDeltaPointer() { return deltaPointer; }
    /**
     * Sets the delta pointer for the queue message.
     *
     * @param deltaPointer the delta pointer
     */
    public void setDeltaPointer(String deltaPointer) { this.deltaPointer = deltaPointer; }
    /**
     * Gets the name of the component that called the queue message.
     *
     * @return the caller name
     */
    public String getCalledBy() { return calledBy; }
    /**
     * Sets the name of the component that called the queue message.
     *
     * @param calledBy the caller name
     */
    public void setCalledBy(String calledBy) { this.calledBy = calledBy; }
    /**
     * Gets the caller reason associated with the queue message.
     *
     * @return the caller reason
     */
    public String getCallerReason() { return callerReason; }
    /**
     * Sets the caller reason associated with the queue message.
     *
     * @param callerReason the caller reason
     */
    public void setCallerReason(String callerReason) { this.callerReason = callerReason; }

    /**
     * Gets the caller reason as the corresponding enum value.
     *
     * @return the parsed caller reason enum, or {@code null} if no reason is set
     */
    public CalledReason getCallerReasonEnum() { return CalledReason.fromString(this.callerReason); }
}