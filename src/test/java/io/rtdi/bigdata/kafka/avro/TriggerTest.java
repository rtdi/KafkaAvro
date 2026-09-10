package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DayOfWeek;
import java.util.Arrays;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.objects.Trigger;
import io.rtdi.bigdata.kafka.avro.objects.Trigger.EventSet;

/**
 * Test all operations around Trigger documents
 */
public class TriggerTest {

	/**
	 * Constructor for the test class
	 */
	public TriggerTest() {
	}

	/**
	 * Prepares the test environment before any tests are run.
	 * @throws Exception if something goes wrong
	 */
	@BeforeAll
	public static void setUp() throws Exception {
	}

	/**
	 * Tears down the test environment after all tests are run.
	 * @throws Exception if something goes wrong
	 */
	@AfterAll
	public static void tearDown() throws Exception {
	}

	/**
	 * Run the tests.
	 */
	@Test
	public void test() {
		try {
			Trigger trigger = new Trigger("function1", "queue1");
			EventSet e = trigger.addEventSet();
			e.addSchedule(Arrays.asList(DayOfWeek.MONDAY, DayOfWeek.WEDNESDAY), Arrays.asList(6, 18), Arrays.asList(0, 30), Arrays.asList(1,2,3,4,5,6,7,8,9,10), Arrays.asList(1,4,7,10), true);
			e.addDataflowEvent("PreviousDF");
			e.addDataflow("thisDF", false, Arrays.asList(0,1,2));
			e.addCommitEvent(0, 20, "Schema1");


			String json = trigger.toRecordJson();
			GenericData.Record triggerrecord = trigger.toRecord();
			Trigger trigger2 = Trigger.fromRecordJson(json);
			Trigger trigger3 = Trigger.from(triggerrecord);

			assertEquals(trigger2, trigger);
			assertEquals(trigger3, trigger2);

			Path path = Path.of("src/test/resources", "trigger_values.json");
			Files.writeString(path, json);

			ObjectMapper om = new ObjectMapper();

			JsonNode json_tree = om.readTree(json);
			path = Path.of("src/test/resources/expected", "trigger_values.json");
			JsonNode expected_tree = om.readTree(path.toFile());
			assertEquals(expected_tree, json_tree, "The built object is different from the expected object");

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
