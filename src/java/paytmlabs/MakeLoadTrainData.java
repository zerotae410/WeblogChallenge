package paytmlabs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * This is for making train data to make predicting load model. The output data
 * is time for minutes and load.
 */
public class MakeLoadTrainData extends Configured implements Tool {

	/**
	 * Extract timestamp and count from log, and set key(time in minute),
	 * value(request count).
	 */
	public static class MakeTrainDataMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static Pattern TOKENS_DELIMITER = Pattern.compile("\"?\\s\"?");
		private DateTimeFormatter timeFormat = ISODateTimeFormat.dateTime();
		private LongWritable timeInMinute = new LongWritable();
		private Text keyTime = new Text();
		private IntWritable valueCount = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] logTokens = TOKENS_DELIMITER.split(value.toString());
			// timestamp
			DateTime timestamp = timeFormat.parseDateTime(logTokens[0]);
			long time_in_mill = timestamp.getMillis();
			// discard seconds
			timeInMinute.set(time_in_mill - (time_in_mill % (20 * 1000)));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			String strTime = sdf.format(new Date(timeInMinute.get())).toString();
			// key(time_in_minute), value(count)
			keyTime.set(strTime);
			context.write(keyTime, valueCount);
		}
	}

	public static class MakeLoadTrainDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable oputputCount = new IntWritable(1);

		public void reduce(Text time_in_minute, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			oputputCount.set(count);
			context.write(time_in_minute, oputputCount);
		}
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new paytmlabs.MakeLoadTrainData(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WeblogChallenge : make train data for predicting loads for next minute");
		job.setJarByClass(MakeLoadTrainData.class);
		job.setMapperClass(MakeTrainDataMapper.class);
		job.setReducerClass(MakeLoadTrainDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
