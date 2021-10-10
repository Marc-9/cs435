package cs435.P1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

public class InConnections {
    public static class OutMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> test = Arrays.asList(value.toString().split(" "));
            context.write(new Text(test.get(0)), new IntWritable(1));
        }
    }
    public static class OutReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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

    public static class Edge{
        String key;
        int count;

        Edge(Text key, int count){
            this.key = key.toString();
            this.count = count;
        }
    }

    public static class CustomComparator implements Comparator<Edge>{
        @Override
        public int compare(Edge o1, Edge o2){
            if (o1.count > o2.count){
                return -1;
            }
            else if(o1.count < o2.count){
                return 1;
            }
            else{
                return o1.key.compareTo(o2.key);
            }
        }
    }

    public static class InMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> test = Arrays.asList(value.toString().split(" "));
            context.write(new Text(test.get(1)), new IntWritable(1));
        }
    }
    public static class InReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "out connections");
        job.setJarByClass(InConnections.class);
        job.setMapperClass(InConnections.OutMapper.class);
        job.setReducerClass(InConnections.OutReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf, "in connections");
        job2.setJarByClass(InConnections.class);
        job2.setMapperClass(InConnections.InMapper.class);
        job2.setReducerClass(InConnections.InReducer.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}