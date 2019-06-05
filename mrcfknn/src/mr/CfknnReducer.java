package mr;


import java.io.IOException;

import main.Instance;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CfknnReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException ,InterruptedException {
		for (Text t : values){
			context.write(NullWritable.get(), t);
		}
	}


}
