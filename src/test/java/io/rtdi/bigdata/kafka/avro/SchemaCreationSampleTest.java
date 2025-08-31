package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.fail;

import org.apache.avro.Schema;
import org.apache.avro.SchemaFormatter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroDecimal;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class SchemaCreationSampleTest {

	@BeforeAll
	public static void setUp() throws Exception {
	}

	@AfterAll
	public static void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			ValueSchema value = new ValueSchema("CUSTOMER", null);
			value.add("CUSTOMER_ID", AvroInt.getSchema(), null, false);
			value.add("COMPANY_NAME", AvroNVarchar.getSchema(30), null, true);
			value.add("ADDRESS_ID", AvroInt.getSchema(), null, true);
			value.add("EMPLOYEES", AvroInt.getSchema(), null, true);
			value.add("REVENUE$", AvroDecimal.getSchema(12, 0), null, true);
			value.setPrimaryKey("CUSTOMER_ID");
			value.build();
			Schema actualschema = value.getSchema();
			System.out.println(SchemaFormatter.format("json/pretty", actualschema));

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}

