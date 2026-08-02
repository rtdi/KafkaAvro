package io.rtdi.bigdata.kafka.avro.objects;

/**
 * A dataflow is started for either of these events
 */
public enum CalledReason {
    /**
     * The dataflow was started manually by a user.
     */
    MANUAL,
    /**
     * The dataflow was started automatically because a previous dataflow was completed
     */
    DATAFLOW,
    /**
     * The dataflow was started because a commit was received.
     */
    COMMIT,
    /**
     * The dataflow was started according to a schedule.
     */
    SCHEDULE;

    /**
     * Parses a string into the matching enum value.
     *
     * @param s the string representation of the enum value
     * @return the matching {@link CalledReason}, or {@code null} when the input is {@code null}
     */
    public static CalledReason fromString(String s) {
        if (s == null) return null;
        return CalledReason.valueOf(s);
    }
}