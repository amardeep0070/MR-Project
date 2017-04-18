package PreProcess;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PreProcessReducer extends Reducer<MyKey,Text,Text,NullWritable> {
  public void reduce(MyKey key, Iterable<Text> values,
                     Context context
  ) throws IOException, InterruptedException {
    for(Text val: values){
      context.write(val, NullWritable.get());
    }
  }
}
