package paytmlabs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * This is for getting the longest session time for ip. This works on Hadoop. To
 * execute this, export this project to jar file. And then, run it like "hadoop
 * WeblogChallenge.jar paytmlabs.SessionLongest {input} {output}"
 */
public class SessionLongest extends Configured implements Tool {

	/**
	 * Calculate the longest session time
	 */
	public static class SessionLongestReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		// fixed time window
		private final static long FIFTEEN_MINUTE = 15 * 60 * 1000L;
		// output value
		private LongWritable session = new LongWritable();

		public void reduce(Text ip, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			// sort timestamp values
			List<Long> list = new ArrayList<Long>();
			for (LongWritable val : values) {
				list.add(val.get());
			}
			Collections.sort(list, new Comparator<Long>() {
				public int compare(Long value1, Long value2) {
					return (int) (value1 - value2);
				}
			});
			long longestSession = 0L;
			long tmpSession = 0L;
			long beginSessionTime = 0L;
			long endSessionTime = 0L;
			long lastTime = 0L;
			for (Long newTime : list) {
				if (newTime > lastTime + FIFTEEN_MINUTE) {
					// new session
					if (endSessionTime > 0) {
						tmpSession = (endSessionTime - beginSessionTime);
						if (tmpSession > longestSession) {
							longestSession = tmpSession;
						}
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
				tmpSession = (endSessionTime - beginSessionTime);
				if (tmpSession > longestSession) {
					longestSession = tmpSession;
				}
			}
			session.set(longestSession);
			context.write(ip, session);
		}
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new paytmlabs.SessionLongest(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WeblogChallenge Goal 3, the longest session");
		job.setJarByClass(SessionLongest.class);
		job.setMapperClass(SessionizeMapper.class);
		job.setReducerClass(SessionLongestReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
