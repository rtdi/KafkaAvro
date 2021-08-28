package io.rtdi.bigdata.kafka.avro;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Schema names are very restrictive, essentially limited to a-z and numbers. Only special character allowed is the _ char.
 * Hence all names need to be encoded.
 *
 */
public class SchemaNameEncoder {
	static Pattern encoderpattern = Pattern.compile("[^A-Za-z0-9_\\.]"); // dots are allowed also
	static Pattern decoderpattern = Pattern.compile("_x[0-9a-f][0-9a-f][0-9a-f][0-9a-f]");


	/**
	 * Encode a string into a-z chars, escaping all other chars.
	 * @param s input string
	 * @return encoded string with escape chars
	 */
	public static String encodeName(String s) {
		s = s.replace("_x", "_x005f_x0078");
		Matcher m = encoderpattern.matcher(s);
		StringBuffer buf = new StringBuffer(s.length());
		while (m.find()) {
			String ch = m.group();
			m.appendReplacement(buf, "_x");
			buf.append(String.format("%1$04x",ch.codePointAt(0)));
		}
		m.appendTail(buf);
		return buf.toString();		
	}

	/** Inverse operation to {@link #encodeName(String)}
	 * 
	 * @param name encoded name
	 * @return decoded name
	 */
	public static String decodeName(String name) {
		Matcher m = decoderpattern.matcher(name);
		StringBuffer buf = new StringBuffer(name.length());
		while (m.find()) {
			m.appendReplacement(buf, "");
			String ch = m.group().substring(2); // _x0065 is to be replaced
			int utf16char = Integer.parseInt(ch, 16);
			buf.append(Character.toChars(utf16char));
		}
		m.appendTail(buf);
		return buf.toString();		
	}

}
