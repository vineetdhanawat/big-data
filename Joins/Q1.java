// Parameters users.dat ratings.dat 661 output

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;

@SuppressWarnings("deprecation")
public class Q1 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		// Used for Map side join
		private HashMap<Integer, String> userData = new HashMap<Integer, String>();
		int movieID;
		private Text keyText = new Text();

		@Override
		public void setup(Context context) throws IOException,	InterruptedException {
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			Configuration conf = context.getConfiguration();
			movieID = Integer.parseInt(conf.get("movieID"));

			File myFile = new File(files[0].getName());
			BufferedReader br = new BufferedReader(new FileReader(myFile));

			String str = null;
			// Building the HashMap
			while ((str = br.readLine()) != null) {
				String[] tokens = str.split("\\::");
				if (tokens[1].equals("M")) {
					String value = "";
					userData.put(Integer.parseInt(tokens[0].trim()), value);
				}
			}
			br.close();
		}

		public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
			String line = value.toString();

			// split using :: as the delimiter
			String[] tokens = line.split("\\::");
			int userID = Integer.parseInt(tokens[0]);
			int mID = Integer.parseInt(tokens[1]);

			IntWritable valueInt = new IntWritable();
			keyText.set(tokens[1]);
			valueInt.set(1);
			if (mID == movieID) {
				if (userData.containsKey(userID)) {
					context.write(keyText, valueInt); // <movieid,1> as key value pair
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
		private Text output = new Text();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			// Counting no of movies
			// count could have been done at map phase and output at cleanup phase
			// No need of reduce
			for(IntWritable value : values) {
				sum++;
			}
			output.set(sum+"");
			context.write(output, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Smaller file, first input users.dat in the cache
		DistributedCache.addCacheFile(new URI(args[0]), conf);
		conf.set("movieID", args[2]);
		Job job = new Job(conf, "Q1");
		job.setJarByClass(Q1.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
}