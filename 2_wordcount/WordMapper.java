//import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//@Override
//     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//		String line = value.toString();
//        for (String word : line.split("\\W+")) {
//	        if (word.length() > 0) {
//				context.write(new Text(word), new IntWritable(1));
//	        }
//		}
//	}
//}

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Convert the line to a string
		String line = value.toString();

		// Split by comma
		String[] words = line.split(",");

		// Emit each word with count 1
		for (String word : words) {
			if (!word.isEmpty()) {
				context.write(new Text(word.trim()), new IntWritable(1));
			}
		}
	}
}
