// Parameters ratings.dat output 30

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Q3 {

	// Map sends userid as key and one as value to Reduce
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text userID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// read the lines from ratings.dat
			String line = value.toString();
			// //split using :: as the delimiter
			String tokens[] = line.split("\\::");
			userID.set(tokens[0]); // User ID is at [0] position.
			context.write(userID, one);
		}
	}

	// Reduce calculates if the user has rated movie more than n times
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private int valueofn;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			String input = conf.get("valueofn");
			valueofn = Integer.parseInt(input);
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			if (sum > valueofn) {
				context.write(key, new IntWritable(sum));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("valueofn", args[2]);
		Job job = new Job(conf, "Q3");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(Q3.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
