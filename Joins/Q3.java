// Parameters ratings.dat temp users.dat output

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Q3 {

    // First Map-Reduce phase to output <userID,one> key value pair
    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
			String[] count = line.split("\\::"); //The tokens are extracted from input ratings file
			word.set(count[0]);
			context.write(word, one); // input to reducer1 <userID,one> key value pair
		}
	}

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Reducer Q1 outputs intermediate file with key value pair <userID,count>
    	@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
    		int sum = 0;
            for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum)); // printing the <userID,count> to an intermediate file
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    	
    	// Map2 access the in memory file users as cached in the memory. 
    	// It also performs join logic on userId key producing <userID,gender+age> key value pair
        
    	private HashMap<Integer, String> userMap = new HashMap<Integer, String>();
 
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			URI[] localUris = DistributedCache.getCacheFiles(context.getConfiguration());
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String localPath = "./MemoryUsers";
            
            //copy the file from cache to a local file and parse it
			fs.copyToLocalFile(new Path(localUris[0]), new Path(localPath)); 
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new FileReader(localPath));
			String word = null;
			int userID;
			String gender;
            
			// fetch userID record from local memory file
			while ((word = reader.readLine()) != null) {
            	String[] tokens = word.trim().split("::");
				userID = Integer.parseInt(tokens[0]);
				// gender contains gender + age
				gender = tokens[1] + "\t" + tokens[2];
				// Copying all the information of userId and <gender+age> in a  memory file
				userMap.put(userID, gender); 
			}
		}

		@Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			// fetches values from Reduce1 <userID,sum> and we join it with  memory user file on userID key as comparison key value.
			// Extract the count from intermediate output
			String[] count = line.trim().split("[\\s]+"); 
			int userID = Integer.parseInt(count[0]);
			Text tempkey = new Text("1");
			Text tempValue = new Text();

			if(userMap.containsKey(userID)){
        		tempValue.set(userID + "\t" + count[1] + "\t" + userMap.get(userID));
        		// output key value <"1",userID and (age+ gender)>
        		context.write(tempkey, tempValue);             	
			}
		}
	}

	//Creating new comparator to implement descending sorting by value and not key
	@SuppressWarnings("rawtypes")
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

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

	// Reduce2 reduces on userID key and perform average descending sorting of records by count
	private static Map<String, Double> countMap = new HashMap<String, Double>(); // map to be sorted
	private static Map<String, String> TotalMap = new HashMap<String, String>(); // lookup map by key after sorting
	private Text finalValue = new Text();
	private Text finalKey = new Text();

	@SuppressWarnings("rawtypes")
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			// fetch all records by same key i.e. "1" from Mapper2.
			String[] userRecords = value.toString().trim().split("\\t"); 
			// this map will be sorted by count
			countMap.put(userRecords[0], Double.parseDouble(userRecords[1])); 
			// this map is referenced for the entire records
			TotalMap.put(userRecords[0], userRecords[3] + "\t" + userRecords[2] + "\t" + userRecords[1]); 
		}

		@SuppressWarnings("unchecked")
		// using descSortMap we will sort the map in a descending order
		Map<String, Double> descSortedMap = new TreeMap(new ValueComparator(countMap));
		// A tree map with sorted count values in order of values as parsed by the ValueComparator.
		descSortedMap.putAll(countMap);
		int count = 0;

		for (Map.Entry<String, Double> entry : descSortedMap.entrySet()) {
			count++;
			finalKey.set(entry.getKey());
			finalValue.set(TotalMap.get(entry.getKey()));
			context.write(finalKey, finalValue);
			if (count == 10) {
				break;
			}

		}
	}
}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "job1");

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setJarByClass(Q1.class);


		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		if (job1.waitForCompletion(true)) {
			
			Configuration conf2 = new Configuration();
			//placing the users file in cache/memory
			DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf2); 
			Job job2 = new Job(conf2, "job2");
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setJarByClass(Q1.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			job2.waitForCompletion(true);
		}
	}
}


