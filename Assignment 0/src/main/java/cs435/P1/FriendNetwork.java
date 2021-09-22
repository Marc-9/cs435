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
import java.util.TreeMap;
import java.util.Comparator;
public class FriendNetwork {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> test = Arrays.asList(value.toString().split(" "));
            if(Float.parseFloat(test.get(0)) > Float.parseFloat(test.get(1))){
                String temp = test.get(0) + " " + test.get(1);
                context.write(new Text(temp), new IntWritable(1));
            }
            else{
                String temp = test.get(1) + " " + test.get(0);
                context.write(new Text(temp), new IntWritable(1));
            }
        }
    }
    public static class CountReducer extends Reducer<Text, IntWritable, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            // calculate the total count
            for(IntWritable val : values){
                count += val.get();
            }
            if(count > 1){
                List<String> test = Arrays.asList(key.toString().split(" "));
                context.write(new Text(test.get(0)), new Text(test.get(1)));
            }

        }

    }
    public static class UselessMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("0"), value);
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, Text> {
        private Map < Text, Text > inNodes;
        @Override
        protected void setup(Context context) {
            inNodes = new TreeMap < Text, Text >();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for(Text val : value){
                List<String> test = Arrays.asList(val.toString().split("	"));
                inNodes.put(new Text(test.get(0)), new Text(test.get(1)));
            }

        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<Text, Text> entry : inNodes.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "find fiends");
        job.setJarByClass(FriendNetwork.class);
        job.setMapperClass(FriendNetwork.TokenizerMapper.class);
        job.setReducerClass(FriendNetwork.CountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf, "sort network");
        job2.setJarByClass(FriendNetwork.class);
        job2.setMapperClass(FriendNetwork.UselessMapper.class);
        job2.setReducerClass(FriendNetwork.SortReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}