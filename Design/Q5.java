// Parameters users.dat temp output

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Q5 {
	// Map1 sends ZipCode as key and Age as value to Reduce1
	public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private DoubleWritable age = new DoubleWritable();
		private Text zip = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// read the lines from users.dat
			String line = value.toString();
			// split using :: as the delimiter
			String tokens[] = line.split("\\::");
			zip.set(tokens[4].trim());
			age.set(Integer.parseInt(tokens[2]));
			context.write(zip, age);
		}
	}

	// Reduce1 calculates the average age of each zipcode using ZipCode as the key
	public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		// private static DoubleWritable avg = new DoubleWritable();
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			// initialize sum and count to 0
			double sum = 0;
			double count = 0;
			DoubleWritable avg = new DoubleWritable(0.0);

			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			avg.set(sum / count);
			context.write(key, avg);
		}
	}

	// Map2 reads the output of Reduce 1 and stores the result into TreeMap for sorting

	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(one, value);
		}
	}

	// Creating a class that will be used as a key in the TreeMap. We will make	a composite key here.
	static class Zip_Age {

		private String zip;
		private double Age;

		public Zip_Age(String zip, double avgAge) {
			this.zip = zip;
			this.Age = avgAge;
		}

		public String getZip() {
			return zip;
		}

		public void setZip(String zip) {
			this.zip = zip;
		}

		public double getAge() {
			return Age;
		}

		public void setAge(double Age) {
			this.Age = Age;
		}
	}

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, DoubleWritable> {

		// creating a treemap that will take the composite key Zip_Age as key and value as Age.
		TreeMap<Zip_Age, Double> AscSort = new TreeMap<Zip_Age, Double>(new Comparekey());

		// Using Comparator to compare the composite key that we created so that
		// it will be sorted in ascending order.
		static class Comparekey implements Comparator<Zip_Age> {

			public int compare(Zip_Age keyA, Zip_Age keyB) {
				if (keyA.getAge() == keyB.getAge() || keyA.getAge() > keyB.getAge()) {
					return 1;
				} else {
					return -1;
				}
			}
		}

		public static DoubleWritable average_double = new DoubleWritable();
		// public static DoubleWritable distinct = new DoubleWritable();
		private String[] average = new String[100];
		private String[] zip = new String[100];
		int count = 0;

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				String[] line = value.toString().split("[\\s]+");
				String ZipCode = line[0];
				Double Avg_Age = Double.parseDouble(line[1]);
				AscSort.put(new Zip_Age(ZipCode, Avg_Age), Avg_Age);
			}

			for (Map.Entry<Zip_Age, Double> entry : AscSort.entrySet()) {

				// If we want distinct values, we can use equal function
				// if(!entry.getValue().equals(distinct)){
				zip[count] = entry.getKey().getZip().toString();
				average[count] = entry.getValue().toString();
				average_double.set(Double.parseDouble(average[count]));
				context.write(new Text(zip[count]), average_double);
				count++;

				if (count == 11) {
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// create job1 to calculate average age of each zipcode
		Job job1 = new Job(conf, "average");

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setJarByClass(Q5.class);

		job1.setMapperClass(Map1.class);
		job1.setCombinerClass(Reduce1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		if (job1.isSuccessful()) {

			Configuration conf2 = new Configuration();
			Job job2 = new Job(conf2, "sort");

			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			job2.setJarByClass(Q5.class);

			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			job2.setNumReduceTasks(1);
			job2.waitForCompletion(true);

		}
	}
}
