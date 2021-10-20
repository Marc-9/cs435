package cs435.P2;
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
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;

public class AdjacencyList {
	public static class DoubleMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			List<String> test = Arrays.asList(value.toString().split(" "));
			context.write(new Text(test.get(0)), new Text(test.get(1) + '\t' + String.valueOf(1)));
			context.write(new Text(test.get(1)), new Text(test.get(0) + '\t' + String.valueOf(1)));
		}
	}



	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AdjacencyList");
		job.setJarByClass(AdjacencyList.class);
		job.setMapperClass(AdjacencyList.DoubleMapper.class);

		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/friendsMini"));
		FileOutputFormat.setOutputPath(job, new Path("/mini1"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}