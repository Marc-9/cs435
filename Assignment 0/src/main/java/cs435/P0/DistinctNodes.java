package cs435.P0;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class DistinctNodes {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text node;
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				node = new Text( s ); 
				context.write(node, NullWritable.get());
			}
 		}
	}	
	public static class CountReducer extends Reducer<Text, NullWritable, IntWritable, NullWritable>{
		private Set<String> distinctNodes ;
		@Override
		protected void setup(Context context) {
			distinctNodes = new HashSet<String>();
		 }
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
			distinctNodes.add(key.toString());
		 }
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			IntWritable noOfNodes = new IntWritable(distinctNodes.size());
			context.write(noOfNodes, NullWritable.get());
 		}
	}	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "distinct nodes new");
		job.setJarByClass(DistinctNodes.class);
		job.setMapperClass(DistinctNodes.TokenizerMapper.class);
		job.setReducerClass(DistinctNodes.CountReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2])); 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
