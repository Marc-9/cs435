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

public class FriendNetwork {
    public static class OutMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> test = Arrays.asList(value.toString().split(" "));
            if(Float.parseFloat(test.get(0)) < Float.parseFloat(test.get(1))){
                String temp = test.get(0) + " " + test.get(1);
                context.write(new Text(temp), new IntWritable(1));
            }
            else{
                String temp = test.get(1) + " " + test.get(0);
                context.write(new Text(temp), new IntWritable(1));
            }
        }
    }
    public static class OutReducer extends Reducer<Text, IntWritable, Text, Text>{
        List<Friends> sortedList;
        @Override
        protected void setup(Context context) {
            sortedList = new ArrayList<Friends>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int count = 0;
            for(IntWritable val : values){
                count += val.get();
            }
            if(count > 1){
                List<String> test = Arrays.asList(key.toString().split(" "));
                sortedList.add(new Friends(test.get(0), test.get(1)));
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(sortedList, new CustomComparator());

            for(int i = 0; i < sortedList.size(); i++) {
                context.write(new Text(sortedList.get(i).key), new Text(sortedList.get(i).key2));
            }
        }
    }

    public static class Friends{
        String key;
        String key2;

        Friends(String key, String key2){
            this.key = key;
            this.key2 = key2;
        }
    }

    public static class CustomComparator implements Comparator<Friends>{
        @Override
        public int compare(Friends o1, Friends o2){
            if (o1.key.compareTo(o2.key) == 0){
                return o1.key2.compareTo(o2.key2);
            }
            else{
                return o1.key.compareTo(o2.key);
            }
        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "friend network");
        job.setJarByClass(FriendNetwork.class);
        job.setMapperClass(FriendNetwork.OutMapper.class);
        job.setReducerClass(FriendNetwork.OutReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}