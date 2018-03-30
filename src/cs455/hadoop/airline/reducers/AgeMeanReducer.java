package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AgeMeanReducer extends Reducer<Text, MeanWritable, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<MeanWritable> values, Context context) throws IOException, InterruptedException {
    int count = 0;
    long sum = 0;
    for(MeanWritable value: values) {
      count += value.count;
      sum += value.sum;
    }
    if(count == 0) {
      return;
    }

    String out = String.format(
        "%.2f",
        (float)sum/count
    );

    context.write(key, new Text(out));
  }
}
