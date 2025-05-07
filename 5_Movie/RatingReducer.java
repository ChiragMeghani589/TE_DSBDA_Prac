import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RatingReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		float sum = 0;
		for(FloatWritable value : values){
			count++;
			sum += value.get();
		}
		float rating = sum/count;
		context.write(key, new FloatWritable(rating));
	}
}
