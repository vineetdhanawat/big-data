// Parameters ratings.dat temp output

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

public class Q2 {

    //Building first Map-Reduce pair to get average ratings
    public static class Q1Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private static IntWritable rating = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] movieRatings = line.split("\\::"); //Extract the tokens from text
            word.set(movieRatings[1]);
            rating.set(Integer.parseInt(movieRatings[2]));
            context.write(word, rating); //write to context -> input to reducer1
        }
    }

    public static class Q1Reduce1 extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private static DoubleWritable avg = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            avg.set(sum / count);
            context.write(key, avg); // printing the movie - avg Rating to intermediate file
        }
    }

    //Building Map-Reduce pair 2 for sorting in desc order and printing top 10
    public static class Q1Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one, value);
        }
    }

    //Creating new comparator to describe sorting by value and not key
    static class ValueComparator implements Comparator {

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

    public static class Q1Reduce2 extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        private static Map<String, Double> movieMap = new HashMap();
        private Text word = new Text();
        private static DoubleWritable avg = new DoubleWritable();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                String[] line = value.toString().split("[\\s]+");
                String movie = line[0];
                Double avgRating = Double.parseDouble(line[1]);
                movieMap.put(movie, avgRating);
            }

            Map<String, Double> descSortedMap = new TreeMap(new ValueComparator(movieMap));
            //creating a tree map sorted by desc order of values as driven by the ValueComparator compare handle
            descSortedMap.putAll(movieMap);
            int count = 0;

            //iterate through descending sorted map and print top 10 to output file
            for (Map.Entry<String, Double> entry : descSortedMap.entrySet()) {
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

    public static void main(String[] args) throws Exception {

        //Setting driver configuration defining Mapper,Reducer classes and respective output files

        Configuration conf = new Configuration();
        Job jobAv = new Job(conf, "Q1Top10Avg");


        jobAv.setMapperClass(Q1Map1.class);
        jobAv.setReducerClass(Q1Reduce1.class);
        jobAv.setMapOutputKeyClass(Text.class);
        jobAv.setMapOutputValueClass(IntWritable.class);
        jobAv.setOutputKeyClass(Text.class);
        jobAv.setOutputValueClass(DoubleWritable.class);
        jobAv.setJarByClass(Q2.class);


        jobAv.setInputFormatClass(TextInputFormat.class);
        jobAv.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobAv, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobAv, new Path(args[1]));

        boolean waitMapperReduce1 = jobAv.waitForCompletion(true);

        if (waitMapperReduce1) {
            Configuration conf2 = new Configuration();
            Job jobTop10 = new Job(conf2, "Q1Top10Avg");


            jobTop10.setMapperClass(Q1Map2.class);
            jobTop10.setReducerClass(Q1Reduce2.class);
            jobTop10.setMapOutputKeyClass(IntWritable.class);
            jobTop10.setMapOutputValueClass(Text.class);
            jobTop10.setOutputKeyClass(Text.class);
            jobTop10.setOutputValueClass(DoubleWritable.class);
            jobTop10.setJarByClass(Q2.class);


            jobTop10.setInputFormatClass(TextInputFormat.class);
            jobTop10.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(jobTop10, new Path(args[1]));
            FileOutputFormat.setOutputPath(jobTop10, new Path(args[2]));

            jobTop10.waitForCompletion(true);
        }
    }
}
