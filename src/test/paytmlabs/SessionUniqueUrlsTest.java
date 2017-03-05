package paytmlabs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import paytmlabs.SessionUniqueUrls.SessionUniqueUrlsReducer;
import paytmlabs.SessionUniqueUrls.TwoValuesWritable;

public class SessionUniqueUrlsTest {

	ReduceDriver<Text, TwoValuesWritable, Text, MapWritable> reduceDriver;

	@Before
	public void setUp() {
		SessionUniqueUrlsReducer reducer = new SessionUniqueUrlsReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testReducer() throws Exception {
		String url1 = "https://paytm.com:443/shop1";
		String url2 = "https://paytm.com:443/shop2";
		String url3 = "https://paytm.com:443/shop3";
		List<TwoValuesWritable> values = new ArrayList<>();
		values.add(new TwoValuesWritable(1437555628019L, url1));
		// add 5 second later
		values.add(new TwoValuesWritable(1437555628019L + (5 * 1000L), url1));
		// add 10 second later
		values.add(new TwoValuesWritable(1437555628019L + (10 * 1000L), url2));
		// add 20 minute later
		values.add(new TwoValuesWritable(1437555628019L + (20 * 60 * 1000L), url2));
		// add 40 minute later
		values.add(new TwoValuesWritable(1437555628019L + (40 * 60 * 1000L), url1));
		// add 41 minute later
		values.add(new TwoValuesWritable(1437555628019L + (41 * 60 * 1000L), url2));
		// add 42 minute later
		values.add(new TwoValuesWritable(1437555628019L + (42 * 60 * 1000L), url3));
		reduceDriver.withInput(new Text("123.242.248.130"), values);
		// 3 sessions
		MapWritable outputMap = new MapWritable();
		// session1 has 2 unique urls
		outputMap.put(new IntWritable(1), new IntWritable(2));
		// session2 has 1 unique urls
		outputMap.put(new IntWritable(2), new IntWritable(1));
		// session3 has 3 unique urls
		outputMap.put(new IntWritable(3), new IntWritable(3));
		reduceDriver.withOutput(new Text("123.242.248.130"), outputMap);
		reduceDriver.runTest();
	}
}
