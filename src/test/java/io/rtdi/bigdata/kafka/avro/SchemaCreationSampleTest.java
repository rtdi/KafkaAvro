package io.rtdi.bigdata.kafka.avro;

import static org.junit.Assert.*;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.kafka.avro.datatypes.*;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class SchemaCreationSampleTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			SchemaBuilder builder = new SchemaBuilder("CUSTOMER", null);
			builder.add("CUSTOMER_ID", AvroInt.getSchema(), null, false).setPrimaryKey();
			builder.add("COMPANY_NAME", AvroNVarchar.getSchema(30), null, true);
			builder.add("ADDRESS_ID", AvroInt.getSchema(), null, true);
			builder.add("EMPLOYEES", AvroInt.getSchema(), null, true);
			builder.add("REVENUE$", AvroDecimal.getSchema(12, 0), null, true);
			builder.build();
			Schema actualschema = builder.getSchema();
			System.out.println(actualschema.toString(true));
			
			ValueSchema value = new ValueSchema("CUSTOMER", null);
			value.add("CUSTOMER_ID", AvroInt.getSchema(), null, false).setPrimaryKey();
			value.add("COMPANY_NAME", AvroNVarchar.getSchema(30), null, true);
			value.add("ADDRESS_ID", AvroInt.getSchema(), null, true);
			value.add("EMPLOYEES", AvroInt.getSchema(), null, true);
			value.add("REVENUE$", AvroDecimal.getSchema(12, 0), null, true);
			value.build();
			actualschema = value.getSchema();
			System.out.println(actualschema.toString(true));

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}

