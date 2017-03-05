package paytmlabs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import paytmlabs.Sessionize.SessionizeMapper;
import paytmlabs.Sessionize.SessionizeReducer;

public class SessionizeTest {

	private final static String SAMPLE_LOG = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2";
	MapDriver<Object, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setUp() {
		SessionizeMapper mapper = new SessionizeMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		SessionizeReducer reducer = new SessionizeReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() throws Exception {
		mapDriver.withInput(new Text(""), new Text(SAMPLE_LOG));
		// 2015-07-22T09:00:28.019143Z == 1437555628019L 
		mapDriver.withOutput(new Text("123.242.248.130"), new LongWritable(1437555628019L));
		mapDriver.runTest();
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
		// add 40 minute later
		values.add(new LongWritable(1437555628019L + (40 * 60 * 1000L)));
		reduceDriver.withInput(new Text("123.242.248.130"), values);
		// 3 sessions
		reduceDriver.withOutput(new Text("123.242.248.130"), new LongWritable(3));
		reduceDriver.runTest();
	}
}
