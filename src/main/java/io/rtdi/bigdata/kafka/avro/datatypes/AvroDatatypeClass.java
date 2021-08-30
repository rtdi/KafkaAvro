package io.rtdi.bigdata.kafka.avro.datatypes;

/**
 * Used to categorize data types into number, text, date,...
 *
 */
public enum AvroDatatypeClass {
	TEXTASCII(0),
	TEXTUNICODE(1),
	
	NUMBER(0),
	
	BINARY(0),
	
	COMPLEX(0),
	
	BOOLEAN(0),
	
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
