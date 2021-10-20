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

public class CountTriangles {
    public static class TriangleMapper extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            List<String> nodes = Arrays.asList(value.toString().split("\\s+"));
            Collections.sort(nodes, new Comparator<String>() {
                @Override
                public int compare(String s1, String s2){
                    return s1.compareToIgnoreCase(s2);
                }
            });
            context.write(new Text(String.join("-", nodes)), new IntWritable(1));
        }
    }

    public static class TriangleReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int count = 0;
            for(IntWritable val : values){
                count += val.get();
            }
            if(count == 3){
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count triangles");
        job.setJarByClass(CountTriangles.class);
        job.setMapperClass(TriangleMapper.class);
        job.setReducerClass(TriangleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path("/triples3"));
        FileOutputFormat.setOutputPath(job, new Path("/triangles"));
        job.waitForCompletion(true);
    }
}

