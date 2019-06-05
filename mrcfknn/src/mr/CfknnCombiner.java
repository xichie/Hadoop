package mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CfknnCombiner extends Reducer<NullWritable, Text, NullWritable, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException,InterruptedException {
        for (Text t : values){
            context.write(NullWritable.get(),t);
        }
    }
}
