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
import org.apache.hadoop.fs.Path;
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
	                    readFile(file);
	                }
	            }
	        } catch(IOException ex) {
	            System.err.println("Exception in mapper setup: " + ex.getMessage());
	        }    
	    }
	    private void readFile(Path filePath) {
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

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for (Map.Entry<String, List<String>> entry : adjacencyList.entrySet()) {
    			String key = entry.getKey();
    			List<String> value = entry.getValue();
    			String listString = String.join(", ", value);
    			context.write(new Text(key), new Text(listString));
			}

		}
	}


	public static void main(String[] args) throws Exception {


		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Replication Join");
		job.setJarByClass(JoinFiles.class);


		DistributedCache.addCacheFile(new Path(args[3]).toUri(), job.getConfiguration());

		job.setMapperClass(ReplicatedJoinMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// No need to reduce
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}