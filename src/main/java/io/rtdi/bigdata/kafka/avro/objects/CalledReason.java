package io.rtdi.bigdata.kafka.avro.objects;

public enum CalledReason {
    MANUAL, DATAFLOW, COMMIT, SCHEDULE;

    /**
     * Executes the CalledReason fromString operation and returns the resulting value.
     * @param s the parameter value
     * @return the resulting value
     */
    public static CalledReason fromString(String s) {
        if (s == null) return null;
        return CalledReason.valueOf(s);
    }
}