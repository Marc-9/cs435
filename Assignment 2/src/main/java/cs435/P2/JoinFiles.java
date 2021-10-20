package cs435.P2;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.IntWritable;
import java.io.*;
import java.util.*;


public class JoinFiles {


	public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {

		Map<String, List<String>> adjacencyList = new HashMap<>();
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			try{
				Path[] pathLength1 = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if(pathLength1 != null && pathLength1.length > 0) {
					for(Path file : pathLength1) {
						readFile(file, context);
					}
				}
			} catch(IOException ex) {
				System.err.println("Exception in mapper setup: " + ex.getMessage());
			}
		}
		private void readFile(Path filePath, Context context) throws IOException, InterruptedException{
			try{
				BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
				String stopWord = null;
				while((stopWord = bufferedReader.readLine()) != null) {
					String[] splited = stopWord.split("\\s+");
					List<String> temp = new ArrayList<String>();

					if(adjacencyList.containsKey(splited[0])){
						temp = adjacencyList.get(splited[0]);
					}
					temp.add(splited[1]);
					adjacencyList.put(splited[0], temp);
				}
			} catch(IOException ex) {
				System.err.println("Exception while reading stop words file: " + ex.getMessage());
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<String> test = Arrays.asList(value.toString().split("\\s+"));
			List<String> connections = adjacencyList.get(test.get(1));
			Map<String, Integer> seenAlready = new HashMap<>();
			int pathLength = Integer.parseInt(test.get(2)) + 1;
			for(String node : connections){
				if(test.get(0).equals(node) || seenAlready.containsKey(node)){
					continue;
				}
				else{
					seenAlready.put(node, 1);
					context.write(new Text(test.get(0)), new Text(node + "\t" + String.valueOf(pathLength)));
				}
			}
		}

	}

	protected static class DeDupeMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			List<String> test = Arrays.asList(value.toString().split("\\s+"));
			int pathLength = Integer.parseInt(test.get(2));
			if(test.get(0).compareTo(test.get(1)) > 0){
				context.write(new Text(test.get(1) + "-" + test.get(0)), new IntWritable(pathLength));
			}
			else{
				context.write(new Text(test.get(0) + "-" + test.get(1)), new IntWritable(pathLength));
			}
		}
	}

	public static class SelectMin extends Reducer<Text, IntWritable, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int minLength = 9;
			for(IntWritable val : values){
				if(val.get() < minLength){
					minLength = val.get();
				}
			}
			List<String> paths = Arrays.asList(key.toString().split("-"));
			context.write(new Text(paths.get(0)), new Text(paths.get(1) + "\t" + String.valueOf(minLength)));
			context.write(new Text(paths.get(1)), new Text(paths.get(0) + "\t" + String.valueOf(minLength)));
		}
	}

	public static class SelectMin2 extends Reducer<Text, IntWritable, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int minLength = 9;
			for(IntWritable val : values){
				if(val.get() < minLength){
					minLength = val.get();
				}
			}
			List<String> paths = Arrays.asList(key.toString().split("-"));
			context.write(new Text(paths.get(0)), new Text(paths.get(1) + "\t" + String.valueOf(minLength)));
		}
	}

	protected static class SumPathsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			List<String> test = Arrays.asList(value.toString().split("\\s+"));
			context.write(new IntWritable(Integer.parseInt(test.get(2))), new IntWritable(1));
		}
	}

	protected static class CountPathReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable val : values){
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}


	public static void main(String[] args) throws Exception {

		for(int i = 2; i < 9; i++){
			Configuration conf = new Configuration();
			String cfName = "Path Length " + i;
			Job job = Job.getInstance(conf, cfName);

			job.setJarByClass(JoinFiles.class);
			DistributedCache.addCacheFile(new Path("/mini1/part-m-00000").toUri(), job.getConfiguration());
			job.setMapperClass(ReplicatedJoinMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(0);
			String pathInput = "";
			if(i == 2){
				pathInput = "/mini" + (i-1);
			}
			else{
				pathInput = "/miniDeDuped" + (i-1);
			}
			FileInputFormat.addInputPath(job, new Path(pathInput));
			String pathOutput = "/mini" + i;
			FileOutputFormat.setOutputPath(job, new Path(pathOutput));

			job.waitForCompletion(true);

			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2, "De-Dupe");

			job2.setJarByClass(JoinFiles.class);
			job2.setMapperClass(DeDupeMapper.class);
			job2.setReducerClass(SelectMin.class);
			job2.setNumReduceTasks(1);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);

			for(int j = 1; j <= i ; j++){
				String path = "/mini" + String.valueOf(j);
				MultipleInputs.addInputPath(job2, new Path(path), TextInputFormat.class);
			}
			FileOutputFormat.setOutputPath(job2, new Path("/miniDeDuped" + i));

			job2.waitForCompletion(true);

		}

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "De-Dupe");

		job2.setJarByClass(JoinFiles.class);
		job2.setMapperClass(DeDupeMapper.class);
		job2.setReducerClass(SelectMin2.class);
		job2.setNumReduceTasks(1);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		for(int j = 1; j <= 8 ; j++){
			String path = "/mini" + String.valueOf(j);
			MultipleInputs.addInputPath(job2, new Path(path), TextInputFormat.class);
		}
		FileOutputFormat.setOutputPath(job2, new Path("/minifinalList"));
		job2.waitForCompletion(true);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Count Paths");

		job.setJarByClass(JoinFiles.class);
		job.setMapperClass(SumPathsMapper.class);
		job.setReducerClass(CountPathReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/minifinalList"));
		FileOutputFormat.setOutputPath(job, new Path("/minitotCount"));

		job.waitForCompletion(true);

	}
}
