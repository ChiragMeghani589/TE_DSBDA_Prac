import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RatingMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] fields = line.split(",");
		if (fields[0].equals("userId")) return;
		try{
			if(fields.length == 4){
				String movieId = fields[1];
				float rating = Float.parseFloat(fields[2]);
				context.write(new Text(movieId), new FloatWritable(rating));
			}
		} catch(Exception e) {
			// skip row
		}
	}
}
