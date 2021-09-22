package cs435.P1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.*;
public class InConnections {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> test = Arrays.asList(value.toString().split(" "));
            context.write(new Text(test.get(0)), new IntWritable(1));
        }
    }
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        List<Edge> sortedList;
        @Override
        protected void setup(Context context) {
            sortedList = new ArrayList<Edge>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int count = 0;
            // calculate the total count
            for(IntWritable val : values){
                count += val.get();
            }
            //context.write(key, new IntWritable(count));
            sortedList.add(new Edge(key, count));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(sortedList, new CustomComparator());
            //Arrays.sort(sortedList);
            for(int i = 0; i < sortedList.size(); i++) {
                context.write(new Text(sortedList.get(i).key), new IntWritable(sortedList.get(i).count));
            }
        }
    }

    public static class Edge implements Comparable<Edge>{
        String key;
        int count;

        Edge(Text key, int count){
            this.key = key.toString();
            this.count = count;
        }
        @Override
        public int compareTo(Edge o){
            if (this.count > o.count){
                return 1;
            }
            else if(this.count < o.count){
                return -1;
            }
            else{
                return this.key.compareTo(o.key);
            }
        }
    }

    public static class CustomComparator implements Comparator<Edge>{
        @Override
        public int compare(Edge o1, Edge o2){
            if (o1.count > o2.count){
                return 1;
            }
            else if(o1.count < o2.count){
                return -1;
            }
            else{
                return o1.key.compareTo(o2.key);
            }
        }
    }

    public static class UselessMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("0"), value);
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, IntWritable> {
        private Map < Text, IntWritable > inNodes;
        @Override
        protected void setup(Context context) {
            inNodes = new HashMap < Text, IntWritable >();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for(Text val : value){
                List<String> test = Arrays.asList(val.toString().split("	"));
                inNodes.put(new Text(test.get(0)), new IntWritable(Integer.parseInt(test.get(1))));
            }

        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LinkedHashMap<Text, IntWritable> reverseSortedMap = new LinkedHashMap<>();
            inNodes.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
            for(Map.Entry<Text, IntWritable> entry : reverseSortedMap.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "in connections");
        job.setJarByClass(InConnections.class);
        job.setMapperClass(InConnections.TokenizerMapper.class);
        job.setReducerClass(InConnections.CountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf, "sort node");
        job2.setJarByClass(InConnections.class);
        job2.setMapperClass(InConnections.UselessMapper.class);
        job2.setReducerClass(InConnections.SortReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}