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

public class CountEdgesAndVertices {

    public static class CountLines extends Mapper<Object, Text, IntWritable, NullWritable> {
        int lineCount = 0;
        @Override
        protected void map(Object key, Text value, Context context) {
            lineCount += 1;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(lineCount), NullWritable.get());
        }

    }

    public static class CountUnique extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            List<String> test = Arrays.asList(value.toString().split(" "));
            context.write(new Text(test.get(0)), NullWritable.get());
            context.write(new Text(test.get(1)), NullWritable.get());
        }
    }

    public static class KeepUnique extends Reducer<Text, NullWritable, IntWritable, NullWritable>{
        int vertices = 0;
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context){
            vertices += 1;
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new IntWritable(vertices), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count edges");

        job.setJarByClass(CountEdgesAndVertices.class);
        job.setMapperClass(CountLines.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path("/friends"));
        FileOutputFormat.setOutputPath(job, new Path("/edges"));

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "count vertices");

        job2.setJarByClass(CountEdgesAndVertices.class);
        job2.setMapperClass(CountUnique.class);
        job2.setReducerClass(KeepUnique.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path("/friends"));
        FileOutputFormat.setOutputPath(job2, new Path("/vertices"));
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.waitForCompletion(true);

    }
}