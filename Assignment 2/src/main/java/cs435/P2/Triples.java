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
import org.apache.hadoop.io.NullWritable;
import java.io.*;
import java.util.*;

public class Triples {
    public static class TripleMapper extends Mapper<Object, Text, Text, Text> {
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

            for(String node : connections){
                if(test.get(0).equals(node)){
                    continue;
                }
                else{
                    if(test.get(0).compareTo(node) > 0){
                        context.write(new Text(node + "-" + test.get(1)), new Text(test.get(0)));
                    }
                    else{
                        context.write(new Text(test.get(0) + "-" + test.get(1)), new Text(node));
                    }
                }
            }

        }

    }

    public static class TripleReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            List<String> test = Arrays.asList(key.toString().split("-"));
            List<String> seen = new ArrayList<>();
            for(Text middle: values){
                String ran = middle.toString();
                if(seen.contains(ran)){
                    continue;
                }
                else{
                    seen.add(ran);
                }
                context.write(new Text(test.get(0)), new Text(test.get(1) + "\t" + ran));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "triples");

        job.setJarByClass(Triples.class);
        DistributedCache.addCacheFile(new Path("/path1/part-m-00000").toUri(), job.getConfiguration());
        job.setMapperClass(TripleMapper.class);
        job.setReducerClass(TripleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path("/path1"));

        FileOutputFormat.setOutputPath(job, new Path("/triples3"));

        job.waitForCompletion(true);
    }

}
