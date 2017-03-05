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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import paytmlabs.Sessionize.SessionizeMapper;

/**
 * This is for making train data to make predicting session time model. Train
 * data's format is ip1, 1p2, ip3, ip4, avg session time.
 */
public class MakeSessionTrainData extends Configured implements Tool {
	public static class MakeTrainSessionDataReducer
			extends Reducer<Text, LongWritable, LongArrayWritable, LongWritable> {
		// fixed time window
		private final static long FIFTEEN_MINUTE = 15 * 60 * 1000L;
		private final static Pattern IP_DELIMITER = Pattern.compile("\\.");

		public void reduce(Text ip, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			// output key
			LongArrayWritable outputKey = new LongArrayWritable();
			String[] ipAddressInArray = IP_DELIMITER.split(ip.toString());
			List<LongWritable> ipAddressLong = new ArrayList<>(ipAddressInArray.length);
			for (String ipAddress : ipAddressInArray) {
				ipAddressLong.add(new LongWritable(Long.parseLong(ipAddress)));
			}
			outputKey.set(ipAddressLong.toArray(new LongWritable[ipAddressLong.size()]));

			// sort timestamp values
			List<Long> timeList = new ArrayList<>();
			for (LongWritable val : values) {
				timeList.add(val.get());
			}
			Collections.sort(timeList, new Comparator<Long>() {
				public int compare(Long value1, Long value2) {
					return (int) (value1 - value2);
				}
			});

			int count = 0;
			long toalSessionTime = 0L;
			long beginSessionTime = 0L;
			long endSessionTime = 0L;
			long lastTime = 0L;
			for (Long newTime : timeList) {
				if (newTime > lastTime + FIFTEEN_MINUTE) {
					// new session
					count += 1;
					if (endSessionTime > 0) {
						toalSessionTime = (endSessionTime - beginSessionTime);
					}
					beginSessionTime = newTime;
					endSessionTime = 0L;
				} else {
					endSessionTime = newTime;
				}
				lastTime = newTime;
			}
			// last in session
			if (endSessionTime == lastTime) {
				toalSessionTime = (endSessionTime - beginSessionTime);
			}
			context.write(outputKey, new LongWritable(toalSessionTime / count));
		}
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new paytmlabs.MakeSessionTrainData(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WeblogChallenge : make 4D ip vectors, avg session time, and unique url count");
		job.setJarByClass(MakeSessionTrainData.class);
		job.setMapperClass(SessionizeMapper.class);
		job.setReducerClass(MakeTrainSessionDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class LongArrayWritable extends ArrayWritable {
		public LongArrayWritable() {
			super(LongWritable.class);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			String[] strings = super.toStrings();
			int index = 0;
			for (String s : strings) {
				// last string doesn't need "\t".
				if (index != (strings.length - 1)) {
					sb.append(s).append("\t");
				} else {
					sb.append(s);
				}
				index += 1;
			}
			return sb.toString();
		}
	}
}
