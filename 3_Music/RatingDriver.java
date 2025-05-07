import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RatingDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Job job = new Job();
		job.setJobName("Rating Calculations");
		job.setJarByClass(RatingDriver.class);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(RatingMapper.class);
		job.setReducerClass(RatingReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		boolean success = job.waitForCompletion(true);		
		System.exit(success ? 0 : 1);
	}
}
