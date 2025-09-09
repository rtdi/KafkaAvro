package io.rtdi.bigdata.kafka.avro;

/**
 * The __audit field has Rule Results
 */
public enum RuleResult {
	/**
	 * No problems with the data found
	 */
	PASS,
	/**
	 * Warnings found in the data
	 */
	WARN,
	/**
	 * Data is invalid
	 */
	FAIL;

	/**
	 * Get a default quality score for this result
	 *
	 * @return quality score
	 */
	public float getDefaultQuality() {
		switch (this) {
		case PASS: return 1.0f;
		case FAIL: return 0.0f;
		default: return 0.9f;
		}
	}

	/**
	 * PASS+PASS=PASS, PASS+WARN=WARN, PASS+FAIL=FAIL, WARN+WARN=WARN, WARN+FAIL=FAIL, FAIL+FAIL=FAIL
	 *
	 * @param ruleresult another rule result to aggregate with
	 * @return aggregated result
	 */
	public RuleResult aggregate(RuleResult ruleresult) {
		if (ruleresult == null) {
			return null;
		} else if (this == FAIL || ruleresult == FAIL) {
			return FAIL;
		} else if (this == WARN || ruleresult == WARN) {
			return WARN;
		} else {
			return PASS;
		}
	}

}
