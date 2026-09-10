package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.objects.Commit;
import io.rtdi.bigdata.kafka.avro.objects.Commit.MinMaxOffsets;
import io.rtdi.bigdata.kafka.avro.objects.Commit.TopicOffsets;

/**
 * Test all operations around Trigger documents
 */
public class CommitTest {

	/**
	 * Constructor for the test class
	 */
	public CommitTest() {
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
			Commit commit = new Commit("commit1", "producer1", 376978617078100L, 1, null);

			Map<String, TopicOffsets> topics = new HashMap<>();
			TopicOffsets t = new TopicOffsets("topic1");
			MinMaxOffsets o = new MinMaxOffsets(0, 100, 0);
			HashMap<String, MinMaxOffsets> mo = new HashMap<>();
			mo.put(String.valueOf(o.getPartition()), o);
			t.setOffsets(mo);
			topics.put(t.getTopic_name(), t);
			commit.setTopics(topics);


			String json = commit.toRecordJson();
			GenericData.Record triggerrecord = commit.toRecord();
			Commit commit2 = Commit.fromRecordJson(json);
			Commit commit3 = Commit.from(triggerrecord);

			assertEquals(commit2, commit);
			assertEquals(commit3, commit2);

			Path path = Path.of("src/test/resources", "commit_values.json");
			Files.writeString(path, json);

			ObjectMapper om = new ObjectMapper();

			JsonNode json_tree = om.readTree(json);
			path = Path.of("src/test/resources/expected", "commit_values.json");
			JsonNode expected_tree = om.readTree(path.toFile());
			assertEquals(expected_tree, json_tree, "The built object is different from the expected object");

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
