package paytmlabs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import paytmlabs.SessionAverage.SessionAverageReducer;

public class SessionAverageTest {

	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setUp() {
		SessionAverageReducer reducer = new SessionAverageReducer();
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
		// add 20 minute, 2 second later
		values.add(new LongWritable(1437555628019L + (20 * 60 * 1000L) + (2 * 1000L)));
		// add 40 minute later
		values.add(new LongWritable(1437555628019L + (40 * 60 * 1000L)));
		reduceDriver.withInput(new Text("123.242.248.130"), values);
		// first session for 10 second
		// second session for 2 second
		// third session for 0 second
		// sum = 12, average = 12/3 = 4000ms
		reduceDriver.withOutput(new Text("123.242.248.130"), new LongWritable(4000L));
		reduceDriver.runTest();
	}
}
