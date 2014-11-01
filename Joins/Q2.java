// Parameters ratings.dat movies.dat temp1 temp2 output

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;

public class Q2 {

	// Calculate average rating Map-Reduce job
	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text movie = new Text();
        private static IntWritable rating = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\::");
            movie.set(tokens[1]);
            rating.set(Integer.parseInt(tokens[2]));
            context.write(movie, rating); // <movieID, rating> key value pair
        }
	}

	public static class Reduce1 extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private static DoubleWritable avgRating = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            avgRating.set(sum / count);
            context.write(key, avgRating); // outputs movie,avgRating as input for second mapper
        }
	}

	// sort and produce top 10 ratings
	public static class Map2 extends Mapper<LongWritable, Text, NullWritable, Text> {

		//private final static IntWritable one = new IntWritable(1);
        // value will have actually key,value pair
		public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}

    // Creating new comparator to describe sorting by value and not key
    public static class ValueComparator implements Comparator {
        Map map;
        public ValueComparator(Map map) {
            this.map = map;
        }

        public int compare(Object keyA, Object keyB) {
            Double valueA = (Double) map.get(keyA);
            Double valueB = (Double) map.get(keyB);

            if (valueA == valueB || valueB > valueA) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public static class Reduce2 extends Reducer<NullWritable, Text, Text, DoubleWritable> {

        private static Map<String, Double> movieMap = new HashMap();
        private Text word = new Text();
        private static DoubleWritable avg = new DoubleWritable();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                String[] tokens = value.toString().split("[\\s]+");
                String movie = tokens[0];
                Double avgRating = Double.parseDouble(tokens[1]);
                movieMap.put(movie, avgRating);
            }

            Map<String, Double> descMovieMap = new TreeMap(new ValueComparator(movieMap));
            //creating a tree map sorted by desc order of values as driven by the ValueComparator compare handle
            descMovieMap.putAll(movieMap);

            int count = 0;
            //iterate through descending sorted map and print top 10 to output file
            for (Map.Entry<String, Double> entry : descMovieMap.entrySet()) {
                count++;
                word.set(entry.getKey());
                avg.set(entry.getValue());
                context.write(word, avg);
                if (count == 10) {
                    break;
                }
            }
        }
    }

	// first mapper to process movies.dat file for movie name
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {

		private Text title = new Text();
		private Text movieID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			// split using :: delimiter
			String[] tokens = line.split("\\::");
			title.set("##" + tokens[1]);
			movieID.set(tokens[0]);
			context.write(movieID, title);
		}
	}

	// second mapper to process previous output
	public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {

		private Text title = new Text();
		private Text movieID = new Text();

		public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
			String line = value.toString();

			// split using :: delimiter
			String[] tokens = line.split("\\t");
			title.set("$$");
			movieID.set(tokens[0]);
			context.write(movieID, title);
		}
	}

	// reducer performs reduce side join with hack
	public static class Reduce3 extends Reducer<Text, Text, Text, NullWritable> {

		private Text tmp = new Text();
		private Text title = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> itr = values.iterator();

			boolean aFlag = false;
			boolean bFlag = false;

			while (itr.hasNext()) {
				
				tmp = itr.next();
				if (tmp.toString().contains("##")) {
					aFlag=true;
					title.set(tmp.toString().substring(2));
				} if (tmp.toString().contains("$$")) {
					bFlag=true;
				}
			}
			
			if(aFlag && bFlag)
				context.write(title, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "job1");
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setJarByClass(Q2.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		boolean waitMapperReduce1 = job1.waitForCompletion(true);
		boolean waitMapperReduce2 = false;
		if (waitMapperReduce1) {
			Configuration conf2 = new Configuration();
			Job job2 = new Job(conf2, "job2");
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setMapOutputKeyClass(NullWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			job2.setJarByClass(Q2.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			waitMapperReduce2 = job2.waitForCompletion(true);
		}
		boolean waitMapperReduce3=false;
		
		if (waitMapperReduce1 && waitMapperReduce2) {
			
			Configuration conf3 = new Configuration();
			Job job3 = new Job(conf3, "job3");
			
			MultipleInputs.addInputPath(job3, new Path(args[1]),
					TextInputFormat.class, Map3.class);
			MultipleInputs.addInputPath(job3, new Path(args[3]),
					TextInputFormat.class, Map4.class);
			
			job3.setReducerClass(Reduce3.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(NullWritable.class);
			job3.setJarByClass(Q2.class);
			job3.setOutputFormatClass(TextOutputFormat.class);

			FileOutputFormat.setOutputPath(job3, new Path(args[4]));
			waitMapperReduce3 = job3.waitForCompletion(true);
		}
	}
}