package paytmlabs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
 * This is for getting unique url counts per session in Weblog. This works on
 * Hadoop. To execute this, export this project to jar file. And then, run it
 * like "hadoop WeblogChallenge.jar paytmlabs.SessionUniqueUrls {input}
 * {output}"
 */
public class SessionUniqueUrls extends Configured implements Tool {

	/**
	 * Extract timestamp, ip, and url from log, and set key(ip),
	 * value(timestamp, url).
	 */
	public static class SessionUniqueUrlsMapper extends Mapper<Object, Text, Text, TwoValuesWritable> {
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
			// key(ip), value(timestamp, url)
			context.write(ip, new TwoValuesWritable(timestamp.getMillis(), logTokens[12]));
		}
	}

	/**
	 * Count unique urls per session for ip
	 */
	public static class SessionUniqueUrlsReducer extends Reducer<Text, TwoValuesWritable, Text, MapWritable> {
		// fixed time window
		private final static long FIFTEEN_MINUTE = 15 * 60 * 1000L;
		// output map
		private MapWritable outputMap = new MapWritable() {
			@Override
			public String toString() {
				StringBuilder result = new StringBuilder();
				Set<Writable> keySet = this.keySet();
				for (Object key : keySet) {
					result.append("{" + key.toString() + ":" + this.get(key).toString() + "}");
				}
				return result.toString();
			}
		};

		public void reduce(Text ip, Iterable<TwoValuesWritable> values, Context context)
				throws IOException, InterruptedException {
			// sort timestamp, url values by timestamp
			List<TwoValues> twoValueList = new ArrayList<TwoValues>();
			for (TwoValuesWritable twoVal : values) {
				twoValueList.add(new TwoValues(twoVal.getFirst(), twoVal.getSecond()));
			}
			Collections.sort(twoValueList, new Comparator<TwoValues>() {
				public int compare(TwoValues value1, TwoValues value2) {
					return (int) (value1.getFirst() - value2.getFirst());
				}
			});

			int sessionId = 0;
			long previousTime = 0L;
			// HashSet does not allow to duplicate
			HashMap<Integer, HashSet<String>> sessionUrlsMap = new HashMap<>();
			for (TwoValues twoValue : twoValueList) {
				long newTime = twoValue.getFirst();
				String url = twoValue.getSecond();
				if (newTime > previousTime + FIFTEEN_MINUTE) {
					// new session
					sessionId += 1;
					HashSet<String> urlSet = new HashSet<>();
					urlSet.add(url);
					sessionUrlsMap.put(sessionId, urlSet);
				} else {
					sessionUrlsMap.get(sessionId).add(url);
				}
				previousTime = newTime;
			}
			outputMap.clear();
			sessionUrlsMap.keySet().stream().forEach(seesionId -> outputMap.put(new IntWritable(seesionId),
					new IntWritable(sessionUrlsMap.get(seesionId).size())));
			context.write(ip, outputMap);
		}
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new paytmlabs.SessionUniqueUrls(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WeblogChallenge Goal 4, count unique urls per session");
		job.setJarByClass(SessionUniqueUrls.class);
		job.setMapperClass(SessionUniqueUrlsMapper.class);
		job.setReducerClass(SessionUniqueUrlsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TwoValuesWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TwoValuesWritable implements Writable {

		private long first;
		private String second;

		public TwoValuesWritable() {
			set(first, second);
		}

		public TwoValuesWritable(long first, String second) {
			set(first, second);
		}

		public void set(long first, String second) {
			this.first = first;
			this.second = second;
		}

		public long getFirst() {
			return first;
		}

		public String getSecond() {
			return second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeUTF(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readLong();
			second = in.readUTF();
		}

		@Override
		public String toString() {
			return String.valueOf(first) + ", " + second;
		}
	}

	public static class TwoValues {

		private long first;
		private String second;

		public TwoValues() {
			set(first, second);
		}

		public TwoValues(long first, String second) {
			set(first, second);
		}

		public void set(long first, String second) {
			this.first = first;
			this.second = second;
		}

		public long getFirst() {
			return first;
		}

		public String getSecond() {
			return second;
		}

		@Override
		public String toString() {
			return String.valueOf(first) + ", " + second;
		}
	}

}
