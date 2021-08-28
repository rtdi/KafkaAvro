package io.rtdi.bigdata.kafka.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.text.StringEscapeUtils;

public class AvroUtils {
	
	public static String convertRecordToJson(GenericRecord record) throws IOException {
		return record.toString();
	}
	
	public static String encodeJson(String text) {
		/*
		 * Backspace is replaced with \b
		 * Form feed is replaced with \f
		 * Newline is replaced with \n
		 * Carriage return is replaced with \r
		 * Tab is replaced with \t
		 * Double quote is replaced with \"
		 * Backslash is replaced with \\
		 */
		return StringEscapeUtils.escapeJson(text);
	}

	public static final byte MAGIC_BYTE = 0x0;

	/**
	 * In case this schema is a union of null and something else, it returns the _something else_
	 * 
	 * @param schema of the input
	 * @return schema without the union of null, in case it is just that. Can return an union still.
	 */
	public static Schema getBaseSchema(Schema schema) {
		if (schema == null) {
			return null;
		} else if (schema.getType() == Type.UNION) {
			List<Schema> types = schema.getTypes();
			if (types.size() == 2 && types.get(0).getType() == Type.NULL) {
				return types.get(1);
			} else {
				return schema;
			}
		} else {
			return schema;
		}
	
	}
}
