package paytmlabs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import paytmlabs.SessionLongest.SessionLongestReducer;

public class SessionLongestTest {

	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setUp() {
		SessionLongestReducer reducer = new SessionLongestReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testReducer() throws Exception {
		List<LongWritable> values = new ArrayList<>();
		values.add(new LongWritable(1437555628019L));
		// add 5 second later
		values.add(new LongWritable(1437555628019L + (5 * 1000L)));
		// add 10 second later
		values.add(new LongWritable(1437555628019L + (10 * 1000L)));
		// add 20 minute later
		values.add(new LongWritable(1437555628019L + (20 * 60 * 1000L)));
		// add 22 minute later
		values.add(new LongWritable(1437555628019L + (22 * 60 * 1000L)));
		// add 40 minute later
		values.add(new LongWritable(1437555628019L + (40 * 60 * 1000L)));
		reduceDriver.withInput(new Text("123.242.248.130"), values);
		// first session for 10 second
		// second session for 2 minute
		// third session for 0 second
		// the longest sesion tmie is 2 * 60 * 1000L = 120000L
		reduceDriver.withOutput(new Text("123.242.248.130"), new LongWritable(120000L));
		reduceDriver.runTest();
	}
}
