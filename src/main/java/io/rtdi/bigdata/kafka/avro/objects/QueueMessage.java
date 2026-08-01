package io.rtdi.bigdata.kafka.avro.objects;

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
     * Creates a new instance of this class.
     */
    public QueueMessage() {}

    /**
     * Executes the String getId operation.
     */
    public String getId() { return id; }
    /**
     * Executes the void setId operation.
     * @param id the parameter value
     */
    public void setId(String id) { this.id = id; }
    /**
     * Executes the String getLoadType operation.
     */
    public String getLoadType() { return loadType; }
    /**
     * Executes the void setLoadType operation.
     * @param loadType the parameter value
     */
    public void setLoadType(String loadType) { this.loadType = loadType; }
    /**
     * Executes the String getDataflowName operation.
     */
    public String getDataflowName() { return dataflowName; }
    /**
     * Executes the void setDataflowName operation.
     * @param dataflowName the parameter value
     */
    public void setDataflowName(String dataflowName) { this.dataflowName = dataflowName; }
    /**
     * Executes the Integer getPartition operation.
     */
    public Integer getPartition() { return partition; }
    /**
     * Executes the void setPartition operation.
     * @param partition the parameter value
     */
    public void setPartition(Integer partition) { this.partition = partition; }
    /**
     * Executes the String getCommitId operation.
     */
    public String getCommitId() { return commitId; }
    /**
     * Executes the void setCommitId operation.
     * @param commitId the parameter value
     */
    public void setCommitId(String commitId) { this.commitId = commitId; }
    /**
     * Executes the String getDeltaPointer operation.
     */
    public String getDeltaPointer() { return deltaPointer; }
    /**
     * Executes the void setDeltaPointer operation.
     * @param deltaPointer the parameter value
     */
    public void setDeltaPointer(String deltaPointer) { this.deltaPointer = deltaPointer; }
    /**
     * Executes the String getCalledBy operation.
     */
    public String getCalledBy() { return calledBy; }
    /**
     * Executes the void setCalledBy operation.
     * @param calledBy the parameter value
     */
    public void setCalledBy(String calledBy) { this.calledBy = calledBy; }
    /**
     * Executes the String getCallerReason operation.
     */
    public String getCallerReason() { return callerReason; }
    /**
     * Executes the void setCallerReason operation.
     * @param callerReason the parameter value
     */
    public void setCallerReason(String callerReason) { this.callerReason = callerReason; }

    /**
     * Executes the CalledReason getCallerReasonEnum operation.
     */
    public CalledReason getCallerReasonEnum() { return CalledReason.fromString(this.callerReason); }
}