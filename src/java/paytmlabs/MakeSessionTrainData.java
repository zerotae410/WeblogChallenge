package paytmlabs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

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

import paytmlabs.SessionUniqueUrls.TwoValues;
import paytmlabs.SessionUniqueUrls.TwoValuesWritable;

/**
 * This is for making train data to make predicting session time model. The
 * train data's format is ip, session count, total session time, the longest
 * session time, the request count, avg session time, unique url count.
 */
public class MakeSessionTrainData extends Configured implements Tool {
	public static class MakeSessionTrainDataReducer extends Reducer<Text, TwoValuesWritable, Text, LongArrayWritable> {
		// fixed time window
		private final static long FIFTEEN_MINUTE = 15 * 60 * 1000L;

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

			// HashSet does not allow to duplicate
			HashSet<String> sessionUniqueUrls = new HashSet<>();

			int count = 0;
			long longestSession = 0L;
			long tmpSession = 0L;
			long totalSessionTime = 0L;
			long beginSessionTime = 0L;
			long endSessionTime = 0L;
			long lastTime = 0L;

			for (TwoValues twoValue : twoValueList) {
				long newTime = twoValue.getFirst();
				String url = twoValue.getSecond();
				if (newTime > lastTime + FIFTEEN_MINUTE) {
					// new session
					count += 1;
					if (endSessionTime > 0) {
						tmpSession = (endSessionTime - beginSessionTime);
						totalSessionTime += tmpSession;
						if (tmpSession > longestSession) {
							longestSession = tmpSession;
						}
					}
					beginSessionTime = newTime;
					endSessionTime = 0L;
				} else {
					endSessionTime = newTime;
				}
				sessionUniqueUrls.add(url);
				lastTime = newTime;
			}
			// last in session
			if (endSessionTime == lastTime) {
				tmpSession = (endSessionTime - beginSessionTime);
				totalSessionTime += tmpSession;
				if (tmpSession > longestSession) {
					longestSession = tmpSession;
				}
			}

			// output values
			LongArrayWritable outputValues = new LongArrayWritable();
			List<LongWritable> ouputList = Arrays.asList(
					new LongWritable(count),
					new LongWritable(totalSessionTime),
					new LongWritable(longestSession),
					new LongWritable(twoValueList.size()),
					new LongWritable(totalSessionTime / count),
					new LongWritable(sessionUniqueUrls.size()));

			outputValues.set(ouputList.toArray(new LongWritable[ouputList.size()]));

			context.write(ip, outputValues);
		}
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new paytmlabs.MakeSessionTrainData(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,
				"WeblogChallenge : make train data for predicting avg sesstion time and url count");
		job.setJarByClass(MakeSessionTrainData.class);
		job.setMapperClass(SessionUniqueUrls.SessionUniqueUrlsMapper.class);
		job.setReducerClass(MakeSessionTrainDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TwoValuesWritable.class);
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
			return String.join("\t", super.toStrings());
		}
	}
}
