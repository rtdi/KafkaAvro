package io.rtdi.bigdata.kafka.avro.objects;

public enum CalledReason {
    MANUAL, DATAFLOW, COMMIT, SCHEDULE;

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