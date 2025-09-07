package io.rtdi.bigdata.kafka.avro.datatypes;

/**
 * Used to categorize data types into number, text, date,...
 *
 */
public enum AvroDatatypeClass {
	/**
	 * ASCII text
	 */
	TEXTASCII(0),
	/**
	 * Unicode text
	 */
	TEXTUNICODE(1),

	/**
	 * All numeric types
	 */
	NUMBER(0),

	/**
	 * Binary data
	 */
	BINARY(0),

	/**
	 * All other types
	 */
	COMPLEX(0),

	/**
	 * Boolean type
	 */
	BOOLEAN(0),

	/**
	 * Date and Time type
	 */
	DATETIME(0);

	private int level;

	AvroDatatypeClass(int i) {
		this.level = i;
	}

	/**
	 * @return the level constant for each data type class
	 */
	public int getLevel() {
		return level;
	}


}
