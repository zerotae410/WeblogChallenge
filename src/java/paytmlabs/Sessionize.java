package paytmlabs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * This is for sessionizing Weblog by ip. This works on Hadoop. To execute this,
 * export this project to jar file. And then, run it like "hadoop
 * WeblogChallenge.jar paytmlabs.Sessionize {input} {output}"
 */
public class Sessionize extends Configured implements Tool {

	/**
	 * Extract timestamp and ip from log, and set key(ip), value(timestamp).
	 */
	public static class SessionizeMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static Pattern TOKENS_DELIMITER = Pattern.compile("\"?\\s\"?");
		private final static Pattern PORT_DELIMITER = Pattern.compile(":");
		private DateTimeFormatter timeFormat = ISODateTimeFormat.dateTime();
		private Text ip = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] logTokens = TOKENS_DELIMITER.split(value.toString());
			// timestap
			DateTime timestamp = timeFormat.parseDateTime(logTokens[0]);
			// ip without port
			ip.set(PORT_DELIMITER.split(logTokens[2])[0]);
			// key(ip), value(timestamp)
			context.write(ip, new LongWritable(timestamp.getMillis()));
		}
	}

	/**
	 * Count sessions for ip
	 */
	public static class SessionizeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		// fixed time window
		private final static long FIFTEEN_MINUTE = 15 * 60 * 1000L;
		// output value
		private LongWritable sessionCount = new LongWritable();

		public void reduce(Text ip, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			// sort timestamp values
			List<Long> timeList = new ArrayList<Long>();
			for (LongWritable val : values) {
				timeList.add(val.get());
			}
			Collections.sort(timeList, new Comparator<Long>() {
				public int compare(Long value1, Long value2) {
					return (int) (value1 - value2);
				}
			});

			int count = 0;
			long previousTime = 0L;
			for (Long newTime : timeList) {
				if (newTime > previousTime + FIFTEEN_MINUTE) {
					// new session
					count += 1;
				}
				previousTime = newTime;
			}
			sessionCount.set(count);
			context.write(ip, sessionCount);
		}
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new paytmlabs.Sessionize(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WeblogChallenge Goal 1, Sessionize");
		job.setJarByClass(Sessionize.class);
		job.setMapperClass(SessionizeMapper.class);
		job.setReducerClass(SessionizeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
