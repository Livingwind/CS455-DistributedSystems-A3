package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.DelayCompositeKey;
import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanReducer extends Reducer<DelayCompositeKey, MeanWritable, DelayCompositeKey, DoubleWritable> {
  @Override
  protected void reduce(DelayCompositeKey key, Iterable<MeanWritable> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0;
    long count = 0;
    for(MeanWritable value: values) {
      sum += value.sum;
      count += value.count;
    }
    double mean = (count > 0) ? sum / count : 0;

    context.write(key, new DoubleWritable(mean));
  }
}
