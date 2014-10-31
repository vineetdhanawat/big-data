// Parameters movies.dat output "Jumanji (1995)","Toy Story (1995)"

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Q4 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text genre = new Text();
		
		private String[] inputMovieName;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
            String parameter = conf.get("movies");
            // movies input in csv format
            inputMovieName = parameter.split(",");
            //split using , as the delimiter
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] tokens = line.split("::");
			//split using :: as the delimiter

            // split using | as there may be more than 1 genre.
            String[] genresInFile = tokens[2].trim().split("\\|");
            String movieNameInFile = tokens[1].trim();
			
			// for all the movie names, check all the genre and set them in genre.
			for (int i=0; i<inputMovieName.length; i++) {
				
				if (movieNameInFile.equals(inputMovieName[i])) {
					for(int j = 0; j < genresInFile.length; j++) {
						genre.set(genresInFile[j].trim());
						context.write(genre, one);	
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
        conf.set("movies", args[2]);
		Job job = new Job(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(Q4.class);
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